#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Estrattore Wikidata per artisti (es: Sfera Ebbasta).
- Parte da un titolo Wikipedia (it) e trova l'ID Wikidata
- Scarica l'entità Wikidata
- Estrae proprietà utili per un artista
- Risolve gli ID (Qxxxx) in etichette in italiano (fallback: inglese)
"""
from pprint import pprint
import requests
from functools import lru_cache
from typing import Any, Dict, List, Optional

UA = {"User-Agent": "RichTye-WikiTool/1.0 (contact@example.com)"}

WIKIPEDIA_API = "https://it.wikipedia.org/w/api.php"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{id}.json"

# Mappa delle proprietà utili per un artista
# Proprietà DIRETTE dell'artista (persona/gruppo)
PROPS = {
    "P569": "data_di_nascita",           # date of birth
    "P19":  "luogo_di_nascita",          # place of birth (Qid)
    "P27":  "cittadinanza",              # country of citizenship (Qid)

    "P937": "luogo_di_attivita",         # work location (Qid, multi)  <-- target primario
    "P551": "residenza",                 # residence (Qid, multi)       <-- fortissimo indizio
    "P740": "luogo_di_formazione",       # location of formation (Qid)  <-- forte per gruppi/crew
    "P69":  "formazione_accademica",     # educated at (Qid, multi)     <-- segui P159/P131 dell'ateneo
    "P108": "datore_di_lavoro",          # employer (Qid, multi)        <-- segui P159/P17 dell'organizzazione
    "P463": "membro_di",                 # member of (Qid, multi)       <-- segui P159/P17/P740 del gruppo/org

    "P106": "occupazioni",               # occupation (Qid, multi)
    "P136": "generi",                    # genre (Qid, multi)
    "P625": "coordinate",                # coordinates (lat, lon)

    "P434": "musicbrainz_artist_id",     # MBID
    "P2002": "twitter_username",         # X/Twitter
    "P2003": "instagram_username",       # Instagram
    "P856":  "sito_web",                 # official website (URL)
    "P166":  "premi",                    # awards received (Qid, multi)

    # (opzionale) utili come contesto geografico se presenti
    "P20":  "luogo_di_morte",            # place of death (Qid)
    "P159": "sede_principale_persona",   # headquarters location (raro su persone; più su org)
    "P131": "unita_amministrativa_persona" # located in the admin. territorial entity (raramente su persone)
}

mapping = {
"ART82291002":"99 Posse",
"ART53496045":"Achille Lauro (cantante)",
"ART18853907":"Alfa (rapper)",
"ART64265460":"Anna (rapper)",
"ART75741740":"Articolo 31",
"ART24123617":"Babaman",
"ART40229749":"Baby K",
"ART56320683":"Bassi Maestro",
"ART19605256":"Beba (rapper)",
"ART02666525":"BigMama (rapper)",
"ART03111237":"Brusco (cantante)",
"ART95365016":"",  # Bushwaka (non chiaro)
"ART28846313":"Caneda (rapper)",
"ART27304446":"Caparezza",
"ART70825116":"Capo Plaza",
"ART67409252":"Chadia Rodriguez",
"ART71969350":"Clementino (rapper)",
"ART81071062":"Club Dogo",
"ART78209349":"Coez",
"ART85821920":"Colle der Fomento",
"ART59609037":"Cor Veleno",
"ART46851094":"Dani Faiv",
"ART63985757":"Dargen D'Amico",
"ART96068455":"Dark Polo Gang",
"ART52349448":"",  # Doll Kill (non chiaro)
"ART14383873":"Don Joe",
"ART09119396":"DrefGold",
"ART86549066":"Emis Killa",
"ART57616402":"Ensi",
"ART19729064":"Entics",
"ART76284946":"Ernia",
"ART14073567":"",  # Eva Rea?
"ART25707984":"Fabri Fibra",
"ART07024718":"Fedez",
"ART46711784":"Frah Quintale",
"ART31005348":"Frankie hi-nrg mc",
"ART52465778":"Fred De Palma",
"ART85046033":"Gemitaiz",
"ART87162895":"Geolier",
"ART83125571":"Ghali",
"ART73965015":"Ghemon",
"ART79325822":"Grido",
"ART04141409":"Gué",  # pagina titolo attuale
"ART91515842":"Hell Raton",
"ART59593021":"",  # Hindaco non chiaro
"ART08177154":"Il Tre (rapper)",
"ART57730937":"Inoki",
"ART17812958":"J-Ax",
"ART80977821":"Jack the Smoker",
"ART88792008":"Jake La Furia",
"ART88199433":"",  # Joey Funboy non chiaro
"ART07469279":"Johnny Marsiglia",
"ART88423027":"La Pina",
"ART39344115":"Lazza (rapper)",
"ART05528539":"Luchè",
"ART20729624":"Madame (cantante)",
"ART40433104":"MadMan (rapper)",
"ART16868977":"Mahmood",
"ART61734477":"MamboLosco",
"ART02733420":"Marracash",
"ART63613967":"Massimo Pericolo",
"ART37807199":"Mike24",  # Mike24 non chiaro
"ART43601431":"M¥SS KETA",
"ART51628788":"Miss Simpatia",  # Miss Simpatia non chiaro
"ART48537029":"Mistaman",
"ART66452136":"",  # MISTICO (voce barca a vela) -> non rapper
"ART91352277":"Mondo Marcio",
"ART71846481":"Mr. Rain",
"ART86576759":"Mudimbi",
"ART52272796":"Neffa",
"ART62385172":"Nerone (rapper)",  # Nerone ambigua (opera); il rapper: Nerone (rapper)
"ART07629990":"Nesli",
"ART19060721":"Niky Savage",  # Niky Savage non chiaro
"ART78358659":"Nitro (rapper)",
"ART07127070":"Noyz Narcos",
"ART42220690":"O' Zulù",
"ART12092805":"Papa V",
"ART66932389":"Piotta",
"ART87389753":"Priestess (rapper)",  # attenzione: esiste anche band canadese
"ART08456301":"Rancore (rapper)",
"ART89596800":"Rkomi",
"ART17240256":"Rocco Hunt",
"ART08302616":"Rondo (rapper)",  # Wikip. titolata Rondo (rapper)
"ART04205421":"Rosa Chemical",
"ART74676403":"Rose Villain",
"ART02449272":"Roshelle",
"ART48622722":"Salmo (rapper)",
"ART56967402":"Samuel Heron",
"ART87497821":"Sfera Ebbasta",
"ART98307962":"Shablo",
"ART26418649":"Shade (rapper)",
"ART64850829":"Shiva (rapper)",
"ART41225226":"Skioffi",
"ART28717687":"Slait",
"ART22979236":"Sottotono",
"ART85780419":"Tedua",
"ART88026810":"Thasup",
"ART51721248":"Tony Boy",
"ART57242110":"Tony Effe",
"ART98118784":"Tormento (rapper)",
"ART15560128":"Vacca (rapper)",
"ART57587384":"Willie Peyote",
"ART71515715":"Yendry",
"ART83631935":"Yung Snapp",
}

def get_wikidata_id_from_wikipedia_title(title: str) -> str:
    """Restituisce l'ID Wikidata (Qxxxx) partendo da un titolo su it.wikipedia.org."""
    params = {
        "action": "query",
        "titles": title,
        "prop": "pageprops",
        "format": "json"
    }
    r = requests.get(WIKIPEDIA_API, params=params, headers=UA, timeout=20)
    r.raise_for_status()
    data = r.json()
    page = next(iter(data["query"]["pages"].values()))
    return page["pageprops"]["wikibase_item"]

def fetch_entity(qid: str) -> Dict[str, Any]:
    """Scarica l'entità Wikidata come JSON."""
    r = requests.get(WIKIDATA_ENTITY.format(id=qid), headers=UA, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data["entities"][qid]

@lru_cache(maxsize=4096)
def resolve_label(qid: str, lang_priority=("it", "en")) -> Optional[str]:
    """Dato un Qid o un Pid (ma usiamo Qid), restituisce l'etichetta leggibile."""
    if not qid or not qid.startswith(("Q", "P")):
        return None
    r = requests.get(WIKIDATA_ENTITY.format(id=qid), headers=UA, timeout=30)
    r.raise_for_status()
    ent = r.json()["entities"][qid]
    labels = ent.get("labels", {})
    for lang in lang_priority:
        if lang in labels:
            return labels[lang]["value"]
    # fallback a qualsiasi lingua se proprio
    if labels:
        return next(iter(labels.values()))["value"]
    return None

def extract_claim_values(entity: Dict[str, Any], pid: str) -> List[Any]:
    """Estrae i valori 'grezzi' di una property (potrebbero essere Qid, stringhe, date, coordinate...)."""
    claims = entity.get("claims", {})
    if pid not in claims:
        return []
    vals = []
    for snak in claims[pid]:
        mainsnak = snak.get("mainsnak", {})
        dv = mainsnak.get("datavalue")
        if not dv:
            continue
        v = dv.get("value")
        if v is None:
            continue
        # Tipi possibili:
        # - entity-id -> {"entity-type":"item","numeric-id":...,"id":"Qxxx"}
        # - time -> {"time":"+1992-12-08T00:00:00Z", ...}
        # - monolingualtext / string / url -> str/dict
        # - globecoordinate -> {"latitude":..,"longitude":..}
        if isinstance(v, dict):
            if "id" in v:  # entity id (Qxxx)
                vals.append(v["id"])
            elif "time" in v:  # data
                vals.append(v["time"])
            elif "latitude" in v and "longitude" in v:  # coordinate
                vals.append((v["latitude"], v["longitude"]))
            elif "text" in v:  # monolingual text
                vals.append(v["text"])
            else:
                # URL o altro formato
                vals.append(v.get("url") or v)
        else:
            # stringa semplice (es. username social)
            vals.append(v)
    return vals

def normalize_date(wikidata_time: str) -> str:
    """Converte un timestamp Wikidata '+YYYY-MM-DDT..Z' in 'YYYY-MM-DD' (se possibile)."""
    # Esempio: '+1992-12-07T00:00:00Z'
    if not wikidata_time or not isinstance(wikidata_time, str):
        return wikidata_time
    return wikidata_time.strip("+").split("T")[0]

def fetch_artist_data(title_wikipedia_it: str = "Sfera Ebbasta") -> Dict[str, Any]:
    qid = get_wikidata_id_from_wikipedia_title(title_wikipedia_it)
    entity = fetch_entity(qid)

    result: Dict[str, Any] = {
        "wikidata_id": qid,
        "label": resolve_label(qid),
        "descrizione": None,
    }

    # Descrizione (se disponibile)
    descriptions = entity.get("descriptions", {})
    result["descrizione"] = (
        descriptions.get("it", {}) or descriptions.get("en", {})
    ).get("value")

    # Estrai e risolvi campi secondo PROPS
    for pid, key in PROPS.items():
        raw_vals = extract_claim_values(entity, pid)
        # Post-processing per alcuni tipi
        if pid == "P569":  # data di nascita
            result[key] = normalize_date(raw_vals[0]) if raw_vals else None
        elif pid in {"P19", "P27", "P937", "P106", "P136", "P166"}:
            # Risolvi Qid in etichette umane
            labels = [resolve_label(v) for v in raw_vals]
            # P937, P106, P136, P166 possono essere liste; P19/P27 spesso singoli
            if pid in {"P937", "P106", "P136", "P166"}:
                result[key] = [l for l in labels if l]
            else:
                result[key] = labels[0] if labels else None
        elif pid == "P625":
            result[key] = raw_vals[0] if raw_vals else None  # (lat, lon)
        elif pid in {"P2002", "P2003", "P434", "P856"}:
            # Stringhe/URL diretti
            result[key] = raw_vals[0] if raw_vals else None
        else:
            result[key] = raw_vals

    return result


import requests
import re
from collections import Counter

UA = {"User-Agent": "GeoInfer/1.0 (you@example.com)"}

def get_wikipedia_text(title: str, lang="it"):
    """Scarica il testo in chiaro della pagina Wikipedia."""
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "extracts",
        "explaintext": "1",
        "titles": title,
        "format": "json"
    }
    r = requests.get(url, params=params, headers=UA)
    r.raise_for_status()
    data = r.json()
    page = next(iter(data["query"]["pages"].values()))
    return page.get("extract", "")

def guess_region_from_text(text: str):
    """Conta le occorrenze di città/regioni italiane nel testo."""
    # lista base (puoi espanderla)
    regions = [
        "Milano", "Roma", "Napoli", "Torino", "Bologna", "Genova", "Firenze", "Venezia",
        "Palermo", "Cagliari", "Bari", "La Spezia", "Cinisello", "Lombardia", "Liguria",
        "Sicilia", "Toscana", "Emilia", "Piemonte", "Lazio", "Campania"
    ]
    counts = Counter()
    for reg in regions:
        n = len(re.findall(rf"\b{reg}\b", text, flags=re.IGNORECASE))
        if n > 0:
            counts[reg] = n
    return counts.most_common()

def infer_region_from_wikipedia(title: str):
    text = get_wikipedia_text(title)
    top = guess_region_from_text(text)
    return top

if __name__ == "__main__":
    for id, artist_title in mapping.items():
        data = fetch_artist_data(artist_title)
        pprint(data)
        ranked = infer_region_from_wikipedia(artist_title)
        print(" Frequenze di regioni/località citate su Wikipedia:")
        for name, count in ranked:
            print(f"{name}: {count} occorrenze")

        if ranked:
            print(f"\ Regione/area più menzionata: {ranked[0][0]}")
        
        print("-------------------------------")
