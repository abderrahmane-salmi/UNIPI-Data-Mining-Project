#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wikidata extractor for artists (e.g., Sfera Ebbasta).
- Starts from an Italian Wikipedia title and finds the Wikidata ID
- Downloads the Wikidata entity
- Extracts useful properties for an artist
- Resolves IDs (Qxxxx) into labels in Italian (fallback: English)
"""
import requests
from typing import Any, Dict, List, Optional
import re
from pathlib import Path
import json
from mappings import *
import pandas as pd

# LLM extractor for missing values
try:
    from llm_extractor import LLMExtractor, ArtistInfoSchema
    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False

WIKIPEDIA_API = "https://it.wikipedia.org/w/api.php"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{id}.json"
PROVINCE_PREFIX_RE = re.compile(
    r"^(?:provincia|citt[àa] metropolitana|provincia autonoma)\s+"
    r"(?:di|del|della|dello|dei|degli|delle|dell['’])\s+",
    flags=re.IGNORECASE,
)




"""gender;birth_date;birth_place;nationality;description;active_start;active_end;province;region;country;latitude;longitude"""
class ArtistImputer:
    """Imputer that enriches artists with Wikidata/Wikipedia data."""

    DATE_PROPERTIES = {"P569", "P2031", "P2032"}
    IMPUTED_FILENAME = "artists_imputed.csv"

    def __init__(
        self,
        *,
        user_agent: str = "data_miners/1.0",
        artist_props: Optional[Dict[str, str]] = None,
        location_hint_props: Optional[Dict[str, str]] = None,
        wiki_mapping: Optional[Dict[str, str]] = None,
        id_column: str = "id_author",
        title_column: Optional[str] = "wikipedia_title",
        region_column: Optional[str] = "region",
        overwrite_existing: bool = False,
        copy: bool = True,
        timeout: int = 30,
        log_path: Optional[str] = "log2",
        cache_dir: Optional[str | Path] = None,
        cache_enabled: bool = True,
        use_llm: bool = True,
    ) -> None:
        self.artist_props = dict(artist_props or ARITIST_PROPS)
        self.location_hint_props = tuple((location_hint_props or REGIONAL_HINT_PROPS).values())
        self.wiki_mapping = wiki_mapping or wiki_author_mapping
        self.id_column = id_column
        self.title_column = title_column
        self.region_column = region_column
        self.overwrite_existing = overwrite_existing
        self.copy = copy
        self.timeout = timeout
        self.log_path = log_path
        self.cache_enabled = cache_enabled
        if self.cache_enabled:
            base_cache_dir = Path(cache_dir) if cache_dir is not None else Path(__file__).resolve().parent
            self.cache_dir = base_cache_dir
            self._default_cache_path = base_cache_dir / self.IMPUTED_FILENAME
        else:
            self.cache_dir = None
            self._default_cache_path = None

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})
        self._city_region_lookup = {
            city.lower(): region for city, region in CITY_TO_REGION.items()
        }
        self._title_to_qid: Dict[str, str] = {}
        self._entity_cache: Dict[str, Dict[str, Any]] = {}
        self._wiki_text_cache: Dict[tuple[str, str], str] = {}
        self._region_cache: Dict[str, Optional[str]] = {}
        self._region_prop_column = self.artist_props.get("P131", "province_or_region")
        
        # Initialize LLM extractor if available and enabled
        self.use_llm = use_llm and LLM_AVAILABLE
        if self.use_llm:
            try:
                self._llm_extractor = LLMExtractor(cache_dir=self.cache_dir / ".llm_cache" if self.cache_dir else None)
            except Exception as e:
                print(f"Warning: LLM extractor initialization failed: {e}")
                self.use_llm = False

    def impute_from_wikidata(
        self,
        df: pd.DataFrame,
        *,
        inplace: bool = False,
        cache_dir: Optional[str | Path] = None,
        use_cache: Optional[bool] = None,
    ) -> pd.DataFrame:


        print("ArtistImputer - Test Run")
        print("=" * 50)
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        cache_path = self._resolve_cache_path(cache_dir, use_cache)
        if cache_path and cache_path.exists():
            cached_df = pd.read_csv(cache_path)
            return self._finalize_result(df, cached_df, inplace)

        work_df = df if inplace or not self.copy else df.copy()
        imputed_df = work_df.apply(self._impute_row, axis=1)

        if cache_path:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            imputed_df.to_csv(cache_path, index=False)

        return self._finalize_result(df, imputed_df, inplace)

    def _impute_row(self, row: pd.Series) -> pd.Series:
        title = row[self.id_column]
        wiki_title = self.wiki_mapping.get(title, "")
        print(f"[DEBUG] Processing: {row.get('name', title)} | wiki_title={wiki_title}")
        
        entity = self._entity_from_title(wiki_title) if wiki_title else None
        
        if not entity and not self.use_llm:
            print(f"[DEBUG]   → No Wikidata entity and LLM disabled, skipping")
            self._log_imputation(row.name, row["name"], {}, None)
            return row

        record = self._extract_artist_record(entity) if entity else {}
        print(f"[DEBUG]   → Wikidata record: {list(record.keys()) if record else 'empty'}")
        
        # Check which fields are still missing
        missing_fields = self._get_missing_fields(row, record)
        print(f"[DEBUG]   → Missing fields: {missing_fields}")
        
        # Use LLM to fill missing fields
        if self.use_llm and missing_fields and wiki_title:
            print(f"[DEBUG]   → Calling LLM extractor...")
            llm_record = self._extract_with_llm(wiki_title, title)
            if llm_record:
                filled = []
                for field, value in llm_record.items():
                    if field in missing_fields and value is not None:
                        record[field] = value
                        filled.append(f"{field}={value}")
                print(f"[DEBUG]   → LLM filled: {filled if filled else 'nothing'}")
            else:
                print(f"[DEBUG]   → LLM returned nothing")

        # Handle region specially (backward compatibility)
        current_region = record.get(self._region_prop_column) or record.get("region")
        if self._is_missing(current_region):
            current_region = row.get(self._region_prop_column)

        if current_region:
            record[self._region_prop_column] = current_region
            if self.region_column:
                record.setdefault(self.region_column, current_region)

        applied = self._apply_values(row, record)
        print(f"[DEBUG]   → Applied: {list(applied.keys()) if applied else 'nothing'}")
        region_info = {"value": current_region, "source": "llm" if self.use_llm else "wikidata"} if current_region else None
        self._log_imputation(row.name, row["name"], applied, region_info)
        return row

    def _get_missing_fields(self, row: pd.Series, record: Dict[str, Any]) -> set:
        """Get fields that are missing in both row and record."""
        target_fields = {"region", "birth_date", "birth_place", "active_start", "province"}
        missing = set()
        for field in target_fields:
            record_value = record.get(field)
            row_value = row.get(field) if field in row.index else None
            if self._is_missing(record_value) and self._is_missing(row_value):
                missing.add(field)
        return missing

    def _extract_with_llm(self, wiki_title: str, artist_id: str) -> Optional[Dict[str, Any]]:
        """Extract artist info using LLM from Wikipedia text."""
        if not self.use_llm:
            return None
        
        text = self._fetch_wikipedia_text(wiki_title)
        if not text:
            return None
        
        try:
            info = self._llm_extractor.extract(text, artist_id=artist_id)
            return info.model_dump()
        except Exception as e:
            print(f"LLM extraction failed for {wiki_title}: {e}")
            return None

    def _entity_from_title(self, title: str) -> Optional[Dict[str, Any]]:
        if not isinstance(title, str) or not title.strip():
            return None
        normalized = title.strip()
        qid = self._title_to_qid.get(normalized)
        if not qid:
            qid = self._get_wikidata_id(normalized)
            if not qid:
                return None
            self._title_to_qid[normalized] = qid
        if qid in self._entity_cache:
            return self._entity_cache[qid]
        try:
            response = self._session.get(WIKIDATA_ENTITY.format(id=qid), timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException:
            return None
        entity = response.json().get("entities", {}).get(qid)
        if entity:
            self._entity_cache[qid] = entity
        return entity

    def _get_wikidata_id(self, title: str) -> Optional[str]:
        if not isinstance(title, str) or not title.strip():
            return None
        params = {
            "action": "query",
            "titles": title.strip(),
            "prop": "pageprops",
            "format": "json",
        }
        try:
            response = self._session.get(WIKIPEDIA_API, params=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException:
            return None
        try:
            page = next(iter(response.json()["query"]["pages"].values()))
            return page["pageprops"]["wikibase_item"]
        except (KeyError, StopIteration):
            return None

    def _extract_artist_record(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        record: Dict[str, Any] = {}
        for pid, column in self.artist_props.items():
            values = self._extract_claim_values(entity, pid)
            if not values:
                continue
            if pid == "P625":
                coords = self._parse_coordinates(values[0])
                if coords:
                    record["latitude"], record["longitude"] = coords
                continue
            value = self._simplify_property_value(pid, values[0])
            if value is not None:
                record[column] = value
        return record

    def _extract_claim_values(self, entity: Dict[str, Any], pid: str) -> List[Any]:
        claims = entity.get("claims", {}).get(pid, [])
        values: List[Any] = []
        for snak in claims:
            raw_value = snak.get("mainsnak", {}).get("datavalue", {}).get("value")
            if raw_value is None:
                continue
            if isinstance(raw_value, dict):
                if "id" in raw_value:
                    values.append(raw_value["id"])
                elif "time" in raw_value:
                    values.append(raw_value["time"])
                elif {"latitude", "longitude"} <= raw_value.keys():
                    values.append((raw_value["latitude"], raw_value["longitude"]))
                elif "text" in raw_value:
                    values.append(raw_value["text"])
                else:
                    values.append(raw_value.get("url") or raw_value)
            else:
                values.append(raw_value)
        return values

    def _apply_values(self, row: pd.Series, record: Dict[str, Any]) -> Dict[str, Any]:
        applied: Dict[str, Any] = {}
        for column, value in record.items():
            if value is None:
                continue
            existing = row[column] if column in row else pd.NA
            if self.overwrite_existing or self._is_missing(existing):
                row[column] = value
                applied[column] = value
        return applied


    # Logic to try inferring the region and province

    def _infer_location_from_hints(self, entity: Dict[str, Any]) -> Optional[str]:
        for pid in self.location_hint_props:
            for hint in self._extract_claim_values(entity, pid):
                location = self._get_location(hint)
                if location:
                    return location
        return None

    def _fetch_wikipedia_text(self, title: str, lang: str = "it") -> str:
        if not isinstance(title, str) or not title.strip():
            return ""
        cache_key = (lang, title.strip())
        if cache_key in self._wiki_text_cache:
            return self._wiki_text_cache[cache_key]
        url = f"https://{lang}.wikipedia.org/w/api.php"
        params = {
            "action": "query",
            "prop": "extracts",
            "explaintext": "1",
            "titles": title,
            "format": "json",
        }
        try:
            response = self._session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException:
            return ""
        
        pages = response.json().get("query", {}).get("pages", {})
        page = next(iter(pages.values()), {})
        text = page.get("extract", "") or ""
        self._wiki_text_cache[cache_key] = text
        return text

    def _get_location(self, value: Any) -> Optional[str]:
        if isinstance(value, str) and value.startswith(("Q", "P")):
            if value in self._region_cache:
                return self._region_cache[value]
            label = self._resolve_label(value)
            normalized = self._normalize_region_label(label)
            if not normalized:
                normalized = self._normalize_region_label(self._strip_parenthetical(label))
            self._region_cache[value] = normalized
            return normalized
        if isinstance(value, str):
            normalized = self._normalize_region_label(value)
            if normalized:
                return normalized
            return self._normalize_region_label(self._strip_parenthetical(value))
        return None

    def _simplify_property_value(self, pid: str, value: Any) -> Optional[Any]:
        if value is None:
            return None
        if pid in self.DATE_PROPERTIES and isinstance(value, str):
            return self._normalize_date(value)
        if pid == "P21":
            label = self._resolve_label(value)
            normalized = self._normalize_gender(label)
            return normalized or label
        if isinstance(value, str) and value.startswith(("Q", "P")):
            return self._resolve_label(value)
        return value

    # helpers
    @staticmethod
    def _normalize_gender(label: Optional[str]) -> Optional[str]:
        if not label:
            return None
        mapping = {
            "male": "M",
            "female": "F",
            "maschio": "M",
            "femmina": "F",
            "uomo": "M",
            "donna": "F",
        }
        return mapping.get(label.strip().lower())

    @staticmethod
    def _normalize_date(wikidata_time: str) -> Optional[str]:
        if not isinstance(wikidata_time, str):
            return None
        clean = wikidata_time.strip("+")
        return clean.split("T")[0] if "T" in clean else clean

    @staticmethod
    def _parse_coordinates(value: Any) -> Optional[tuple[float, float]]:
        if isinstance(value, (list, tuple)) and len(value) == 2:
            return float(value[0]), float(value[1])
        if isinstance(value, dict) and {"latitude", "longitude"} <= value.keys():
            return float(value["latitude"]), float(value["longitude"])
        return None

    def _resolve_label(self, value: Any) -> Optional[str]:
        if not isinstance(value, str) or not value.startswith(("Q", "P")):
            return value
        try:
            response = self._session.get(WIKIDATA_ENTITY.format(id=value), timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException:
            return None
        entity = response.json().get("entities", {}).get(value, {})
        labels = entity.get("labels", {})
        for lang in ("it", "en"):
            if lang in labels:
                label = labels[lang]["value"]
                return label
        if labels:
            label = next(iter(labels.values()))["value"]
            return label

        return None

    @staticmethod
    def _is_missing(value: Any) -> bool:
        if isinstance(value, str) and not value.strip():
            return True
        try:
            return pd.isna(value)
        except Exception:
            return False

    def _normalize_location_label(self, label: Optional[str]) -> Optional[str]:
        return self._normalize_region_label(label)

    @staticmethod
    def _strip_parenthetical(label: Optional[str]) -> Optional[str]:
        if not label:
            return None
        base = label.split("(")[0]
        base = base.split(",")[0]
        return base.strip()

    @staticmethod
    def _strip_province_prefix(label: Optional[str]) -> Optional[str]:
        if not label:
            return None
        stripped = PROVINCE_PREFIX_RE.sub("", label).strip()
        return stripped or None

    def _serialize_for_log(self, value: Any) -> Any:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        return str(value)

    def _resolve_cache_path(
        self,
        cache_dir: Optional[str | Path],
        use_cache: Optional[bool],
    ) -> Optional[Path]:
        cache_enabled = self.cache_enabled if use_cache is None else use_cache
        if not cache_enabled:
            return None
        if cache_dir is not None:
            base_dir = Path(cache_dir)
        else:
            base_dir = self.cache_dir
        if base_dir is None:
            return None
        return Path(base_dir) / self.IMPUTED_FILENAME

    def _finalize_result(
        self,
        original_df: pd.DataFrame,
        result_df: pd.DataFrame,
        inplace: bool,
    ) -> pd.DataFrame:
        if inplace:
            original_df[result_df.columns] = result_df
            return original_df
        return result_df


    def _normalize_region_label(self, label: Optional[str]) -> Optional[str]:
        if not label:
            return None
        cleaned = (
            label.replace("–", "-")
            .replace("’", "'")
            .replace("  ", " ")
            .strip()
        )
        candidates: List[str] = []

        def add_candidate(value: Optional[str]) -> None:
            if value and value not in candidates:
                candidates.append(value)

        add_candidate(cleaned)
        add_candidate(self._strip_province_prefix(cleaned))
        dashless = cleaned.replace("-", " ")
        add_candidate(dashless)
        add_candidate(self._strip_province_prefix(dashless))

        for candidate in candidates:
            normalized = self._match_region_candidate(candidate)
            if normalized:
                return normalized
        return None

    def _match_region_candidate(self, candidate: str) -> Optional[str]:
        if candidate in REGIONS:
            return candidate
        if candidate in REGION_SYNONYMS:
            return REGION_SYNONYMS[candidate]
        location_region = CITY_TO_REGION.get(candidate)
        if location_region:
            return location_region
        return self._city_region_lookup.get(candidate.lower())
    
    def _log_imputation(
        self,
        row_index: Any,
        title: Optional[str],
        applied: Dict[str, Any],
        region_info: Optional[Dict[str, Any]],
    ) -> None:
        if not self.log_path:
            return
        if isinstance(row_index, (int, float)) and not pd.isna(row_index):
            serialized: Any = int(row_index)
        else:
            serialized = row_index
        entry: Dict[str, Any] = {
            "row_index": serialized,
            "wiki_title": title,
            "imputed": {k: self._serialize_for_log(v) for k, v in applied.items()},
        }
        if region_info:
            entry["region_info"] = {
                "value": self._serialize_for_log(region_info.get("value")),
                "source": region_info.get("source"),
            }
        try:
            with open(self.log_path, "a", encoding="utf-8") as log_file:
                log_file.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError:
            pass

if __name__ == "__main__":
    artists_df = pd.read_csv("datasets/artists.csv", sep=";")
    columns = artists_df.columns
    imputer = ArtistImputer()
    artists_df = imputer.impute_from_wikidata(artists_df, cache_dir=None, use_cache=False)
    artists_df.to_csv("artists_imputed2.csv", columns=columns)
