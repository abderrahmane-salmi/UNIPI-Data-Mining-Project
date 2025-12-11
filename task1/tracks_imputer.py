"""Ultra-compact MusicBrainz imputer: fill album, date, duration, language."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import lyricsgenius
import pandas as pd
import requests
import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials
from mappings import *

load_dotenv()

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

    IMPUTED_FILENAME = "imputed_tracks.csv"

    def impute(
        self,
        df: pd.DataFrame,
        *,
        inplace: bool = False,
        cache_dir: str | Path | None = None,
    ) -> pd.DataFrame:
        """Impute missing data using all APIs: Spotify -> MusicBrainz -> Genius.
        
        Uses a single cache file that accumulates results progressively.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        
        cache_path: Path | None = None
        if cache_dir is not None:
            cache_dir = Path(cache_dir)
            cache_path = cache_dir / self.IMPUTED_FILENAME
            print(cache_path)
            if cache_path.exists():

                cached_df = pd.read_csv(cache_path)
                return self._finalize_result(df, cached_df, inplace)
        
        work_df = df if inplace or not self.copy else df.copy()
        
        # Each API builds on previous results (no individual cache)
        work_df = self.impute_from_spotify(work_df, inplace=True)
        work_df = self.impute_from_musicbrainz(work_df, inplace=True)
        work_df = self.impute_from_genius(work_df, inplace=True)
        
        # Save final combined result
        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            work_df.to_csv(cache_path, index=False)

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
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        inplace: bool = False,
        cache_dir: str | Path | None = None,
        target_columns: Optional[list] = None,
    ) -> pd.DataFrame:
        """Impute missing track data using the Spotify API via spotipy."""
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        work_df = df if inplace or not self.copy else df.copy()

        self._spotify = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=client_id or os.getenv('SPOTIFY_CLIENT_ID'),
            client_secret=client_secret or os.getenv('SPOTIFY_CLIENT_SECRET')
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
        genius_token: Optional[str] = None,
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
        self._genius_client = lyricsgenius.Genius(genius_token or os.getenv('GENIUS_ACCESS_TOKEN'))
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
        """Impute lyrics for a single row using Genius with validation."""
        if "lyrics" in row.index and not self._is_missing(row["lyrics"]):
            if not self.overwrite_existing:
                return row

        title = self._clean(row.get(self.title_column))
        artist = self._clean(row.get(self.artist_column) or row.get("name_artist"))
        if not title:
            return row

        self._respect_rate_limit()  # Rate limit between searches
        lyrics = self._fetch_lyrics_validated(title, artist)
        if not lyrics:
            return row

        row["lyrics"] = lyrics
        self._log_imputation(row.name, row.get(self.title_column), row.get("id"), {"lyrics": "(lyrics)"})
        return row

    def _fetch_lyrics_validated(self, title: str, artist: Optional[str], min_score: int = 60) -> Optional[str]:
        """Fetch lyrics with fallback and validation."""
        import re, unicodedata
        from difflib import SequenceMatcher
        
        def normalize(text: str) -> str:
            text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode()
            text = re.sub(r'\(.*?\)|\[.*?\]', '', text)
            text = re.sub(r'\b(feat\.?|ft\.?|prod\.?|remix|remaster|explicit|clean|live|version).*', '', text, flags=re.I)
            return ' '.join(re.sub(r'[^\w\s]', '', text).lower().split())
        
        def similarity(a: str, b: str) -> int:
            return int(SequenceMatcher(None, normalize(a), normalize(b)).ratio() * 100)
        
        def validate_song(song, orig_title: str, orig_artist: str) -> bool:
            if not song:
                return False
            title_score = similarity(orig_title, song.title)
            artist_score = similarity(orig_artist, song.artist) if orig_artist else 100
            return ((title_score + artist_score) // 2) >= min_score
        
        norm_title = normalize(title)
        norm_artist = normalize(artist) if artist else ''

        searches = [
            (title, artist),
            (norm_title, norm_artist if norm_artist else None),
        ]
        
        for t, a in searches:
            try:
                song = self._genius_client.search_song(t, a)
                
                if song:
                    if validate_song(song, title, artist or ''):
                        print(f"Found song: {song.title} by {song.artist}, lyrics: {song.lyrics}")
                        return song.lyrics
                else:
                    continue

            except Exception:
                continue
        return None


if __name__ == "__main__":
    print("=" * 50)
    print("TracksImputer - Test Run")
    print("=" * 50)
    
    # Load dataset
    print("\n[1/4] Loading tracks.csv...")
    tracks_df = pd.read_csv("../datasets/imputed_tracks.csv")
    print(f"      Loaded {len(tracks_df)} tracks")
    
    # Initialize imputer
    imputer = TracksImputer()
    cache_dir = "../datasets/"
    
    # Run imputation pipeline
    print("\n[2/4] Running imputation pipeline...")
    print("      Order: Spotify -> MusicBrainz -> Genius")
    print(f"      Cache: {cache_dir}tracks_imputed.csv")
    
    result_df = imputer.impute_from_genius(tracks_df)
    

    print(f"      Rows: {len(result_df)}")
    print(f"      Columns: {len(result_df.columns)}")
    
    sample_cols = ['title', 'album_name', 'duration_ms', 'popularity']
    available_cols = [c for c in sample_cols if c in result_df.columns]
    if available_cols:
        print(result_df[available_cols].head(3).to_string())
    
    print("\n" + "=" * 50)
    print("Done!")
