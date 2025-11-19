import re
drug_words = {
    # English
    "weed", "cocaine", "crack", "heroin", "joint", "blunt", "acid",
    "lsd", "meth", "ecstasy", "molly", "dope", "high", "purple", "pusher", "coke",
    "fentanil"
    # Italian
    "droga", "fumo", "spinello", "erba", "canne", "cocaina", "eroina", "coca", "bianca",
    "lean", "md", "fatto", "metanfetamina", "canna", "spinelli", "trip","abuso","sostanza",
    "sostanze","acidi","acido","pere","ago", "anfetamina", "assenzio","astinenza","ayahuasca",
    "bamba", "benzo", "benzodiazepine", "xanax", "xanny", "goccie", "goccia", "cannone", "bomba",
    "narghilè", "botta", "busta", "roba", "spaccio", "spacciare", "calo", "calarsi", "cala", 
    "cartone", "keta", "ketamina", "dipendenza", "dipendente", "dose", "funghetti", "hashish",
    "pasticca", "pasticche", "tossico", "metadone", "neve", "perquisa","pippo", "pippare","pippato",
    "popper", "rollo", "rollare", "sbronza", "sbronzo", "scitto", "sniffare", "sniffo"
}
violence_words = {
    "uccidere", "uccido", "ammazzare", "ammazzo", "sparare", "sparo", "sparerò", "pistola", "arma", "coltello",
    "violenza", "massacro", "pugno", "botte", "omicidio", "sangue", "tagliare", "taglio",
    "morte", "colpire", "ucciso", "freddo", "freddare", "morto", "coltello", "lama", "sbirro"
}



def explicit_score(lyrics, explicit_words):
    if not isinstance(lyrics, str):
        return 0
    # tokenize roughly by words
    tokens = re.findall(r"\b\w+\b", lyrics.lower())
    # count how many tokens are in the explicit vocabulary
    return sum(word in explicit_words for word in tokens)