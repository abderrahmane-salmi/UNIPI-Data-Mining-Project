#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LLM-based extractor for artist info from Wikipedia text.
Uses LangChain + Google Gemini for structured output extraction.
"""

import os
import json
import hashlib
import time
import re
from pathlib import Path
from typing import Optional, Literal, Dict, Any

import pandas as pd
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

# LangChain imports
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate

# Local imports for validation
from mappings import REGIONS, REGION_SYNONYMS, CITY_TO_REGION
from utils import check_in_set, check_date, check_numeric_range, italian_regions


class ArtistInfoSchema(BaseModel):
    """Schema for extracting artist info from Wikipedia text."""

    birth_date: Optional[str] = Field(
        default=None, 
        description="Date of birth in YYYY-MM-DD format"
    )
    birth_place: Optional[str] = Field(
        default=None, 
        description="City or town of birth"
    )
    active_start: Optional[str] = Field(
        default=None, 
        description="Year career started (YYYY format)"
    )
    province: Optional[str] = Field(
        default=None, 
        description="Italian province (e.g., Milano, Napoli, Roma)"
    )
    region: Optional[str] = Field(
        default=None,
        description="Italian region where the artist is ACTIVE (not birth place). "
        "Must be one of: Lombardia, Lazio, Campania, Sicilia, Piemonte, Veneto, "
        "Emilia-Romagna, Toscana, Puglia, Calabria, Sardegna, Liguria, Marche, "
        "Abruzzo, Friuli-Venezia Giulia, Trentino-Alto Adige, Umbria, "
        "Basilicata, Molise, Valle d'Aosta"
    )


SYSTEM_PROMPT = """You are an assistant that extracts biographical information about Italian rap/hip-hop artists.

IMPORTANT: Think step-by-step before answering.

## REGION EXTRACTION (Chain-of-Thought)
To find the artist's ACTIVE region, follow these steps:

1. **Identify where the artist is CURRENTLY based or most ACTIVE** - look for:
   - Current residence ("vive a...", "si trasferisce a...")
   - Music scene they belong to ("scena milanese", "scena romana", "scena napoletana")
   - Record label location
   - Crew/collective they're part of and where it's based

2. **If no current activity info, check recent career mentions**:
   - Cities frequently mentioned in recent discography
   - Recent tour or concert references
   
3. **Map the city/province to the correct Italian region**:
   - Milano, Monza, Bergamo, Brescia → Lombardia
   - Roma, Latina, Frosinone → Lazio
   - Napoli, Salerno, Avellino, Caserta → Campania
   - Torino, Novara, Cuneo → Piemonte
   - Genova, La Spezia, Savona → Liguria

4. **Validate consistency**: Province MUST be in the correct region!

## OUTPUT RULES
- birth_date: YYYY-MM-DD format (if year only, use YYYY-01-01)
- active_start: Year career started (YYYY format or YYYY-01-01)
- province: Italian province where ACTIVE
- region: Italian region where ACTIVE (must contain the province!)

Valid regions: Lombardia, Lazio, Campania, Sicilia, Piemonte, Veneto, Emilia-Romagna, Toscana, Puglia, Calabria, Sardegna, Liguria, Marche, Abruzzo, Friuli-Venezia Giulia, Trentino-Alto Adige, Umbria, Basilicata, Molise, Valle d'Aosta"""


USER_PROMPT = """Analyze this Wikipedia text about an Italian artist and extract their information.

Think step-by-step about where the artist is ACTIVE (not just born).

Wikipedia text:
{wikipedia_text}"""


class LLMExtractor:
    """Wrapper for LangChain + Gemini structured extraction."""

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        model: str = "gemini-3-flash",
        cache_dir: Optional[str | Path] = None,
        cache_enabled: bool = True,
    ):  

        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found. Set it in .env or pass api_key.")

        self.model = model
        self.cache_enabled = cache_enabled
        self.cache_dir = Path(cache_dir) if cache_dir else Path(__file__).parent / ".llm_cache"

        if self.cache_enabled:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

        self._llm = ChatGoogleGenerativeAI(
            model=self.model,
            api_key=self.api_key,
            temperature=0.2,  
        )
        self._structured_llm = self._llm.with_structured_output(ArtistInfoSchema)

        self._prompt = ChatPromptTemplate.from_messages([
            ("system", SYSTEM_PROMPT),
            ("human", USER_PROMPT),
        ])

    def extract(self, wikipedia_text: str, artist_id: Optional[str] = None) -> ArtistInfoSchema:
        """Extract artist info from Wikipedia text with automatic retry on rate limits.
        
        Args:
            wikipedia_text: The Wikipedia article text.
            artist_id: Optional artist ID for caching.
            
        Returns:
            ArtistInfoSchema with extracted info.
        """
        if not wikipedia_text or not wikipedia_text.strip():
            return ArtistInfoSchema()

        cache_key = self._get_cache_key(wikipedia_text, artist_id)
        if self.cache_enabled:
            cached = self._load_from_cache(cache_key)
            if cached:
                return cached

        # Retry with exponential backoff for rate limits
        max_retries = 5
        base_wait = 20  # seconds
        
        for attempt in range(max_retries):
            try:
                chain = self._prompt | self._structured_llm
                result = chain.invoke({"wikipedia_text": wikipedia_text[:8000]})
                break  # Success, exit retry loop
            except Exception as e:
                error_str = str(e)
                # Check if it's a rate limit error
                if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                    # Extract retry delay from error message if available
                    retry_match = re.search(r'retry in (\d+)', error_str.lower())
                    if retry_match:
                        wait_time = int(retry_match.group(1)) + 5  # Add 5s buffer
                    else:
                        wait_time = base_wait * (2 ** attempt)  # Exponential backoff
                    
                    if attempt < max_retries - 1:
                        print(f"Rate limit hit, waiting {wait_time}s before retry ({attempt + 1}/{max_retries})...")
                        time.sleep(wait_time)
                        continue
                
                print(f"LLM extraction failed: {e}")
                return ArtistInfoSchema()
        else:
            # All retries exhausted
            print(f"LLM extraction failed after {max_retries} retries")
            return ArtistInfoSchema()

        result = self._validate_and_normalize(result)

        if self.cache_enabled:
            self._save_to_cache(cache_key, result)

        return result

    def _validate_and_normalize(self, info: ArtistInfoSchema) -> ArtistInfoSchema:
        """Validate and normalize extracted info using utils.py validation functions."""
        data = info.model_dump()
        
        temp_df = pd.DataFrame([data])
        
        if data["region"]:
            data["region"] = REGION_SYNONYMS.get(data["region"], data["region"])
            temp_df.loc[0, "region"] = data["region"]
        
        if data["region"] is not None:
            errors = check_in_set(temp_df, "region", italian_regions)
            if errors:
                if data["birth_place"]:
                    data["region"] = CITY_TO_REGION.get(data["birth_place"])
                else:
                    data["region"] = None

        if data["birth_date"]:
            try:
                temp_df["birth_date"] = pd.to_datetime(temp_df["birth_date"], errors="coerce")
                errors = check_date(temp_df, "birth_date", "1940-01-01")
                if errors:
                    data["birth_date"] = None
            except Exception:
                data["birth_date"] = None

        return ArtistInfoSchema(**data)

    def _get_cache_key(self, text: str, artist_id: Optional[str]) -> str:
        """Generate cache key from text hash."""
        if artist_id:
            return artist_id
        return hashlib.md5(text.encode()).hexdigest()[:16]

    def _load_from_cache(self, cache_key: str) -> Optional[ArtistInfoSchema]:
        """Load cached result."""
        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return ArtistInfoSchema(**data)
            except Exception:
                return None
        return None

    def _save_to_cache(self, cache_key: str, info: ArtistInfoSchema) -> None:
        """Save result to cache."""
        cache_file = self.cache_dir / f"{cache_key}.json"
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(info.model_dump(), f, ensure_ascii=False, indent=2)
        except Exception:
            pass


if __name__ == "__main__":
    extractor = LLMExtractor()
    
    test_text = """
    Gionata Boschetti, known as Sfera Ebbasta, is an Italian rapper born in Sesto San Giovanni 
    on December 14, 1992. He is considered one of the pioneers of Italian trap. He started 
    his music career in 2011 by publishing his first songs on YouTube.
    """
    
    result = extractor.extract(test_text, artist_id="test_sfera")
    print("Extracted info:")
    print(result.model_dump_json(indent=2))
