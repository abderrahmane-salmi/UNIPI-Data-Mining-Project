"""Ultra-compact MusicBrainz imputer: fill album, date, duration, language."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

import lyricsgenius
import pandas as pd
import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from mappings import *

class TracksImputer:
    MUSICBRAINZ_ENDPOINT = "https://musicbrainz.org/ws/2/recording/"
    SPOTIFY_IMPUTED_FILENAME = "tracks_spotify_imputed.csv"
    GENIUS_IMPUTED_FILENAME = "tracks_genius_imputed.csv"

    def __init__(
        self,
        *,
        user_agent: str = "data_miners/1.0 (tracks-imputer)",
        title_column: str = "title",
        artist_column: str = "primary_artist",
        overwrite_existing: bool = False,
        copy: bool = True,
        timeout: int = 30,
        request_interval: float = 1.1,
        target_columns = IMPUTABLE_EXTRACTORS.keys(),
        log_path: Optional[str] = "tracks_imputer.log",
    ) -> None:
        self.title_column = title_column
        self.artist_column = artist_column
        self.overwrite_existing = overwrite_existing
        self.copy = copy
        self.timeout = timeout
        self.request_interval = max(0.0, request_interval)
        self.target_columns = target_columns
        self.log_path = log_path

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent, "Accept": "application/json"})
        self._last_request = 0.0

    IMPUTED_FILENAME = "tracks_imputed.csv"

    def impute(
        self,
        df: pd.DataFrame,
        *,
        inplace: bool = False,
        cache_dir: str | Path | None = None,
    ) -> pd.DataFrame:
        """Impute missing data using all APIs: Spotify -> MusicBrainz -> Genius."""
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        work_df = df if inplace or not self.copy else df.copy()

        work_df = self.impute_from_spotify(work_df, inplace=True, cache_dir=cache_dir)
        work_df = self.impute_from_musicbrainz(work_df, inplace=True, cache_dir=cache_dir)
        work_df = self.impute_from_genius(work_df, inplace=True, cache_dir=cache_dir)

        return self._finalize_result(df, work_df, inplace)

    def impute_from_musicbrainz(
        self,
        df: pd.DataFrame,
        *,
        inplace: bool = False,
        cache_dir: str | Path | None = None,
    ) -> pd.DataFrame:
        """Impute missing track data using MusicBrainz API."""
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        work_df = df if inplace or not self.copy else df.copy()

        cache_path: Path | None = None
        if cache_dir is not None:
            cache_dir = Path(cache_dir)
            cache_path = cache_dir / self.IMPUTED_FILENAME
            if cache_path.exists():
                cached_df = pd.read_csv(cache_path)
                return self._finalize_result(df, cached_df, inplace)

        imputed_df = work_df.apply(self._impute_row_musicbrainz, axis=1)

        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            imputed_df.to_csv(cache_path, index=False)

        return self._finalize_result(df, imputed_df, inplace)

    def _impute_row_musicbrainz(self, row: pd.Series) -> pd.Series:
        if not self._needs_imputation(row):
            return row
        title = self._clean(row.get(self.title_column))
        artist = self._clean(row.get(self.artist_column) or row.get("name_artist"))
        if not title:
            return row

        recording = self._fetch_recording(title, artist)
        if not recording:
            return row

        applied: Dict[str, Any] = {}
        for column, fn in ((c, f) for c, f in IMPUTABLE_EXTRACTORS.items() if c in self.target_columns):
            if (self.overwrite_existing or self._is_missing(row[column])): 
                value = fn(recording)
                if value:
                    row[column] = fn(recording)
                    applied[column] = fn(recording)
        if applied:
            self._log_imputation(row.name, row.get(self.title_column), row.get("id"), applied)
        return row

    def _fetch_recording(self, title: str, artist: Optional[str]) -> Optional[Dict[str, Any]]:
        query = f'recording:"{title}"'
        if artist:
            query += f' AND artist:"{artist}"'
        params = {"fmt": "json", "query": query, "limit": 1, "inc": "releases"}
        self._respect_rate_limit()
        try:
            response = self._session.get(self.MUSICBRAINZ_ENDPOINT, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError):
            return None
        recordings = data.get("recordings") or []
        return recordings[0] if recordings else None

    def _needs_imputation(self, row: pd.Series) -> bool:
        return any(self._is_missing(row.get(col)) for col in self.target_columns if col in row.index)

    def _respect_rate_limit(self) -> None:
        delta = time.monotonic() - self._last_request
        if delta < self.request_interval:
            time.sleep(self.request_interval - delta)
        self._last_request = time.monotonic()

    @staticmethod
    def _clean(value: Any) -> Optional[str]:
        if isinstance(value, str):
            cleaned = " ".join(value.split())
            return cleaned if cleaned else None
        return None

    @staticmethod
    def _normalize_date(value: Optional[str]) -> Optional[str]:
        if not isinstance(value, str) or not value.strip():
            return None
        parts = value.split("-")
        if len(parts) == 1:
            return parts[0]
        if len(parts) == 2:
            return f"{parts[0]}-{parts[1].zfill(2)}"
        return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"

    @staticmethod
    def _normalize_language(value: Optional[str]) -> Optional[str]:
        if not isinstance(value, str) or not value.strip():
            return None
        return value.lower()[:2]

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _is_missing(value: Any) -> bool:
        if isinstance(value, str):
            return not value.strip()
        return pd.isna(value)

    def _log_imputation(
        self,
        row_index: Any,
        title: Optional[str],
        track_id: Any,
        applied: Dict[str, Any],
    ) -> None:
        if not self.log_path:
            return
        entry = {
            "row_index": int(row_index)
            if isinstance(row_index, (int, float)) and not pd.isna(row_index)
            else row_index,
            "track_id": track_id,
            "title": title,
            "updates": {k: self._serialize(v) for k, v in applied.items()},
        }
        try:
            with open(self.log_path, "a", encoding="utf-8") as log_file:
                log_file.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError:
            pass

    @staticmethod
    def _serialize(value: Any) -> Any:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        return str(value)

    def _finalize_result(
        self, original_df: pd.DataFrame, result_df: pd.DataFrame, inplace: bool
    ) -> pd.DataFrame:
        if not inplace:
            return result_df
        original_df[result_df.columns] = result_df
        return original_df

    def impute_from_spotify(
        self,
        df: pd.DataFrame,
        *,
        client_id: str = "4ed71a1313f248aa838aae7dcce8caef",
        client_secret: str = "010f4b0914fe498dbefd66fa5c9e70e1",
        inplace: bool = False,
        cache_dir: str | Path | None = None,
        target_columns: Optional[list] = None,
    ) -> pd.DataFrame:
        """Impute missing track data using the Spotify API via spotipy."""
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        work_df = df if inplace or not self.copy else df.copy()

        self._spotify = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=client_id, client_secret=client_secret
        ))
        self._spotify_target_columns = target_columns or list(SPOTIFY_EXTRACTORS.keys())

        cache_path: Path | None = None
        if cache_dir is not None:
            cache_dir = Path(cache_dir)
            cache_path = cache_dir / self.SPOTIFY_IMPUTED_FILENAME
            if cache_path.exists():
                return self._finalize_result(df, pd.read_csv(cache_path), inplace)

        imputed_df = work_df.apply(self._impute_row_spotify, axis=1)

        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            imputed_df.to_csv(cache_path, index=False)

        return self._finalize_result(df, imputed_df, inplace)

    def _impute_row_spotify(self, row: pd.Series) -> pd.Series:
        """Impute a single row using Spotify data with Fetch & Validate strategy."""
        if not any(self._is_missing(row.get(c)) for c in self._spotify_target_columns if c in row.index):
            return row

        title = self._clean(row.get(self.title_column))
        artist = self._clean(row.get(self.artist_column) or row.get("name_artist"))
        if not title:
            return row

        self._respect_rate_limit()  # Rate limit between searches, not between fallbacks
        track = self._search_spotify_validated(title, artist)
        if not track:
            return row

        applied: Dict[str, Any] = {}
        for col, fn in ((c, f) for c, f in SPOTIFY_EXTRACTORS.items() if c in self._spotify_target_columns and c in row.index):
            if self.overwrite_existing or self._is_missing(row[col]):
                if (val := fn(track)) is not None:
                    row[col], applied[col] = val, val

        if applied:
            self._log_imputation(row.name, row.get(self.title_column), row.get("id"), applied)
        return row

    def _search_spotify_validated(self, title: str, artist: Optional[str], min_score: int = 70) -> Optional[Dict[str, Any]]:
        """Search Spotify with Fetch & Validate: 2 stages, fuzzy matching on candidates."""
        import re, unicodedata
        from difflib import SequenceMatcher
        
        def normalize(text: str) -> str:
            text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode()
            text = re.sub(r'\(.*?\)|\[.*?\]', '', text)
            text = re.sub(r'\b(feat\.?|ft\.?|prod\.?|remix|remaster|explicit|clean|live|version|edit).*', '', text, flags=re.I)
            return ' '.join(re.sub(r'[^\w\s]', '', text).lower().split())
        
        def similarity(a: str, b: str) -> int:
            return int(SequenceMatcher(None, normalize(a), normalize(b)).ratio() * 100)
        
        def validate_candidates(items: list, orig_title: str, orig_artist: str) -> Optional[Dict[str, Any]]:
            best, best_score = None, 0
            for track in items:
                track_title = track.get('name', '')
                track_artist = track.get('artists', [{}])[0].get('name', '')
                score = (similarity(orig_title, track_title) + similarity(orig_artist, track_artist)) // 2 if orig_artist else similarity(orig_title, track_title)
                if score > best_score:
                    best, best_score = track, score
            return best if best_score >= min_score else None
        
        norm_title, norm_artist = normalize(title), normalize(artist) if artist else ''
        
        # Stage 1: Strict search (original title + artist)
        # Stage 2: Loose search (normalized)
        queries = [
            f'track:"{title}"' + (f' artist:"{artist}"' if artist else ''),
            f'{norm_title} {norm_artist}'.strip(),
        ]
        
        for q in queries:
            try:
                items = self._spotify.search(q=q, type='track', limit=5).get('tracks', {}).get('items', [])
                if items and (match := validate_candidates(items, title, artist or '')):
                    return match
            except Exception:
                continue
        return None

    def impute_from_genius(
        self,
        df: pd.DataFrame,
        *,
        genius_token: str = "59aKiFTVIt5tjAVyDv1f24vJhZ8ymHTcDDonFTuBhrohULgP7eG38hKquCvsSK1s",
        inplace: bool = False,
        cache_dir: str | Path | None = None,
    ) -> pd.DataFrame:
        """Impute missing lyrics using the Genius API.

        Args:
            df: DataFrame with track data.
            genius_token: Genius API access token (required).
            inplace: If True, modify df in place.
            cache_dir: Directory for caching results.

        Returns:
            DataFrame with imputed lyrics.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        work_df = df if inplace or not self.copy else df.copy()

        # Initialize Genius client
        self._genius_client = lyricsgenius.Genius(genius_token)
        self._genius_client.verbose = False
        self._genius_client.remove_section_headers = True

        cache_path: Path | None = None
        if cache_dir is not None:
            cache_dir = Path(cache_dir)
            cache_path = cache_dir / self.GENIUS_IMPUTED_FILENAME
            if cache_path.exists():
                cached_df = pd.read_csv(cache_path)
                return self._finalize_result(df, cached_df, inplace)

        imputed_df = work_df.apply(self._impute_row_genius, axis=1)

        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            imputed_df.to_csv(cache_path, index=False)

        return self._finalize_result(df, imputed_df, inplace)

    def _impute_row_genius(self, row: pd.Series) -> pd.Series:
        """Impute lyrics for a single row using Genius data."""
        # Check if lyrics already exist
        if "lyrics" in row.index and not self._is_missing(row["lyrics"]):
            if not self.overwrite_existing:
                return row

        title = self._clean(row.get(self.title_column))
        artist = self._clean(row.get(self.artist_column) or row.get("name_artist"))
        if not title:
            return row

        # Fetch lyrics from Genius
        lyrics = self._fetch_lyrics(title, artist)
        if not lyrics:
            return row

        row["lyrics"] = lyrics
        self._log_imputation(
            row.name, row.get(self.title_column), row.get("id"), {"lyrics": lyrics}
        )
        return row

    def _fetch_lyrics(self, title: str, artist: Optional[str]) -> Optional[str]:
        """Fetch lyrics from Genius API.

        Args:
            title: Track title.
            artist: Artist name (optional but recommended).

        Returns:
            Lyrics string if found, None otherwise.
        """
        try:
            song = self._genius_client.search_song(title, artist)
            if song and song.lyrics:
                return song.lyrics
        except Exception:
            # Handle timeout, connection errors, etc.
            pass
        return None


if __name__ == "__main__":
    print("Loading tracks.csv...")
    tracks_df = pd.read_csv("../datasets/tracks.csv")
    print(f"Loaded {len(tracks_df)} tracks")
    
    imputer = TracksImputer()
    cache_dir = Path(__file__).resolve().parent / "datasets"
    
    print("\n=== Running full imputation pipeline ===")
    print("1. Spotify -> 2. MusicBrainz -> 3. Genius\n")
    
    result_df = imputer.impute(tracks_df)
    
    print(f"\nDone! Imputed {len(result_df)} tracks")
    print(f"Columns: {list(result_df.columns)}")
