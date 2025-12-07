"""Ultra-compact MusicBrainz imputer: fill album, date, duration, language."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests
from mappings import *

class TracksImputer:
    MUSICBRAINZ_ENDPOINT = "https://musicbrainz.org/ws/2/recording/"
    SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
    SPOTIFY_SEARCH_URL = "https://api.spotify.com/v1/search"
    SPOTIFY_TRACK_URL = "https://api.spotify.com/v1/tracks/{id}"
    SPOTIFY_IMPUTED_FILENAME = "tracks_spotify_imputed.csv"

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

        imputed_df = work_df.apply(self._impute_row, axis=1)

        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            imputed_df.to_csv(cache_path, index=False)

        return self._finalize_result(df, imputed_df, inplace)

    def _impute_row(self, row: pd.Series) -> pd.Series:
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
        client_id: str,
        client_secret: str,
        inplace: bool = False,
        cache_dir: str | Path | None = None,
        target_columns: Optional[list] = None,
    ) -> pd.DataFrame:
        """Impute missing track data using the Spotify API.

        Args:
            df: DataFrame with track data.
            client_id: Spotify API client ID.
            client_secret: Spotify API client secret.
            inplace: If True, modify df in place.
            cache_dir: Directory for caching results.
            target_columns: Columns to impute (defaults to SPOTIFY_EXTRACTORS keys).

        Returns:
            DataFrame with imputed values.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        work_df = df if inplace or not self.copy else df.copy()

        # Get Spotify access token
        self._spotify_token = self._get_spotify_token(client_id, client_secret)
        if not self._spotify_token:
            raise ValueError("Failed to obtain Spotify access token")

        self._spotify_target_columns = target_columns or list(SPOTIFY_EXTRACTORS.keys())

        cache_path: Path | None = None
        if cache_dir is not None:
            cache_dir = Path(cache_dir)
            cache_path = cache_dir / self.SPOTIFY_IMPUTED_FILENAME
            if cache_path.exists():
                cached_df = pd.read_csv(cache_path)
                return self._finalize_result(df, cached_df, inplace)

        imputed_df = work_df.apply(self._impute_row_spotify, axis=1)

        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            imputed_df.to_csv(cache_path, index=False)

        return self._finalize_result(df, imputed_df, inplace)

    def _get_spotify_token(self, client_id: str, client_secret: str) -> Optional[str]:
        """Obtain an access token using the Client Credentials flow."""
        import base64

        credentials = f"{client_id}:{client_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "client_credentials"}

        try:
            response = requests.post(
                self.SPOTIFY_TOKEN_URL,
                headers=headers,
                data=data,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json().get("access_token")
        except (requests.RequestException, ValueError):
            return None

    def _search_spotify_track(
        self, title: str, artist: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Search Spotify for a track and return the first result."""
        query = f'track:"{title}"'
        if artist:
            query += f' artist:"{artist}"'

        headers = {"Authorization": f"Bearer {self._spotify_token}"}
        params = {"q": query, "type": "track", "limit": 1}

        self._respect_rate_limit()
        try:
            response = self._session.get(
                self.SPOTIFY_SEARCH_URL,
                headers=headers,
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError):
            return None

        tracks = data.get("tracks", {}).get("items", [])
        return tracks[0] if tracks else None

    def _fetch_spotify_track(self, track_id: str) -> Optional[Dict[str, Any]]:
        """Fetch full track data by Spotify ID."""
        headers = {"Authorization": f"Bearer {self._spotify_token}"}
        url = self.SPOTIFY_TRACK_URL.format(id=track_id)

        self._respect_rate_limit()
        try:
            response = self._session.get(url, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError):
            return None

    def _impute_row_spotify(self, row: pd.Series) -> pd.Series:
        """Impute a single row using Spotify data."""
        if not self._needs_imputation_spotify(row):
            return row

        title = self._clean(row.get(self.title_column))
        artist = self._clean(row.get(self.artist_column) or row.get("name_artist"))
        if not title:
            return row

        # Search for the track on Spotify
        track = self._search_spotify_track(title, artist)
        if not track:
            return row

        applied: Dict[str, Any] = {}
        for column, fn in (
            (c, f) for c, f in SPOTIFY_EXTRACTORS.items() if c in self._spotify_target_columns
        ):
            if column not in row.index:
                continue
            if self.overwrite_existing or self._is_missing(row[column]):
                value = fn(track)
                if value is not None:
                    row[column] = value
                    applied[column] = value

        if applied:
            self._log_imputation(
                row.name, row.get(self.title_column), row.get("id"), applied
            )
        return row

    def _needs_imputation_spotify(self, row: pd.Series) -> bool:
        """Check if the row needs any Spotify-based imputation."""
        return any(
            self._is_missing(row.get(col))
            for col in self._spotify_target_columns
            if col in row.index
        )


if __name__ == "__main__":
    tracks_df = pd.read_csv("datasets/tracks.csv")
    imputer = TracksImputer()
    imputer.impute(tracks_df, cache_dir=Path(__file__).resolve().parent)

