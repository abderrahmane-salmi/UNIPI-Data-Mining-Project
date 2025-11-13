#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Estrattore Wikidata per artisti (es: Sfera Ebbasta).
- Parte da un titolo Wikipedia (it) e trova l'ID Wikidata
- Scarica l'entità Wikidata
- Estrae proprietà utili per un artista
- Risolve gli ID (Qxxxx) in etichette in italiano (fallback: inglese)
"""
import requests
from typing import Any, Dict, List, Optional
import re
from collections import Counter
import json
from mappings import *
import pandas as pd

WIKIPEDIA_API = "https://it.wikipedia.org/w/api.php"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{id}.json"




"""gender;birth_date;birth_place;nationality;description;active_start;active_end;province;region;country;latitude;longitude"""
class ArtistImputer:
    """Imputer che arricchisce gli artisti con i dati di Wikidata/Wikipedia."""

    DATE_PROPERTIES = {"P569", "P2031", "P2032"}

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
        region_wikipedia_threshold = 1
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
        self.region_wikipedia_threshold = region_wikipedia_threshold

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})
        self._title_to_qid: Dict[str, str] = {}
        self._entity_cache: Dict[str, Dict[str, Any]] = {}
        self._wiki_text_cache: Dict[tuple[str, str], str] = {}
        self._region_cache: Dict[str, Optional[str]] = {}
        self._region_prop_column = self.artist_props.get("P131", "province_or_region")

    def impute_from_wikidata(self, df: pd.DataFrame, *, inplace: bool = False) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        work_df = df if inplace or not self.copy else df.copy()
        work_df = work_df.apply(self._impute_row, axis=1)
        return work_df

    def _impute_row(self, row: pd.Series) -> pd.Series:
        title = row[self.id_column]
        entity = self._entity_from_title(self.wiki_mapping[title])
        if not entity:
            self._log_imputation(row.name, row["name"], {}, None)
            return row

        record = self._extract_artist_record(entity)
        current_region = record.get(self._region_prop_column)
        if self._is_missing(current_region):
            current_region = row.get(self._region_prop_column)

        region_value, region_source = self._resolve_region(
            entity, title, current_region
        )
        if region_value:
            record[self._region_prop_column] = region_value
            if self.region_column:
                record.setdefault(self.region_column, region_value)

        applied = self._apply_values(row, record)
        region_info = {"value": region_value, "source": region_source} if region_value else None
        self._log_imputation(row.name, row["name"], applied, region_info)
        return row

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


    ###logica per provare a inferire la regione e la provincia

    def _resolve_region(
        self,
        entity: Dict[str, Any],
        title: str,
        current_value: Any,
    ) -> tuple[Optional[str], Optional[str]]:
        if not self._is_missing(current_value):
            return current_value, "wikidata_property"
        region = self._infer_location_from_hints(entity)
        if region:
            return region, "regional_hint"
        wiki_region = self._infer_location_from_wikipedia(self.wiki_mapping[title])
        if wiki_region:
            return wiki_region, "wikipedia_text"
        return None, None

    def _infer_location_from_hints(self, entity: Dict[str, Any]) -> Optional[str]:
        for pid in self.location_hint_props:
            for hint in self._extract_claim_values(entity, pid):
                location = self._get_location(hint)
                if location:
                    return location
        return None

    def _infer_location_from_wikipedia(self, title: str) -> Optional[str]:
        ranked = self._rank_locations_from_wikipedia(title)
        if not ranked:
            return None
        
        if len(ranked) == 1:
            normalized = self._normalize_location_label(ranked[0][0])
            if normalized:
                return normalized
            
        elif ranked[0][1] > ranked[1][1] + self.region_wikipedia_threshold:
            normalized = self._normalize_location_label(ranked[0][0])
            if normalized:
                return normalized
                
        return None

    def _rank_locations_from_wikipedia(self, title: str, lang: str = "it") -> List[tuple[str, int]]:
        text = self._fetch_wikipedia_text(title, lang=lang)
        if not text:
            return []
        region_keywords = set(REGIONS)
        region_keywords.update(REGION_SYNONYMS.keys())
        counts = Counter()
        first_seen: Dict[str, int] = {}

        def record_hit(key: str, pos: int) -> None:
            counts[key] += 1
            if key not in first_seen or pos < first_seen[key]:
                first_seen[key] = pos

        for keyword in region_keywords:
            for match in re.finditer(rf"{re.escape(keyword)}", text, flags=re.IGNORECASE):
                record_hit(keyword, match.start())

        for city, region in CITY_TO_REGION.items():
            for match in re.finditer(rf"{re.escape(city)}", text, flags=re.IGNORECASE):
                record_hit(region, match.start())

        return sorted(
            counts.items(),
            key=lambda item: (
                -item[1],
                first_seen.get(item[0], float("inf")),
                item[0],
            ),
        )

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
        if not label:
            return None
        cleaned = (
            label.replace("–", "-")
            .replace("’", "'")
            .replace("  ", " ")
            .strip()
        )
        if cleaned in REGIONS:
            return cleaned
        if cleaned in REGION_SYNONYMS:
            return REGION_SYNONYMS[cleaned]
        variant = cleaned.replace("-", " ")
        if variant in REGION_SYNONYMS:
            return REGION_SYNONYMS[variant]
        return None

    @staticmethod
    def _strip_parenthetical(label: Optional[str]) -> Optional[str]:
        if not label:
            return None
        base = label.split("(")[0]
        base = base.split(",")[0]
        return base.strip()

    def _serialize_for_log(self, value: Any) -> Any:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, pd.Timestamp):
            return value.isoformat()


    def _normalize_region_label(self, label: Optional[str]) -> Optional[str]:
        if not label:
            return None
        cleaned = (
            label.replace("–", "-")
            .replace("’", "'")
            .replace("  ", " ")
            .strip()
        )
        if cleaned in REGIONS or cleaned in CITY_TO_REGION.keys():
            return cleaned
        if cleaned in REGION_SYNONYMS:
            return REGION_SYNONYMS[cleaned]
        variant = cleaned.replace("-", " ")
        if variant in REGION_SYNONYMS:
            return REGION_SYNONYMS[variant]
        return None
    
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
    artists_df = imputer.impute_from_wikidata(artists_df)
    artists_df.to_csv("artists_imputed2.csv", columns=columns)
