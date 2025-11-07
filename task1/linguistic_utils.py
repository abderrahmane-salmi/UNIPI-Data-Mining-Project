import re
drug_words = {
    # English
    "weed", "cocaine", "crack", "heroin", "joint", "blunt", "acid",
    "lsd", "meth", "ecstasy", "molly", "dope", "high",
    # Italian
    "droga", "fumo", "spinello", "erba", "canne", "cocaina", "eroina", "coca", "bianca",
    "lean", "md"
}
violence_words = {
    "uccidere", "ammazzare", "sparare", "pistola", "arma", "coltello",
    "violenza", "massacro", "pugno", "botte", "omicidio", "sangue",
    "morte", "colpire", "ucciso", "freddo", "freddare"
}



def explicit_score(lyrics, explicit_words):
    if not isinstance(lyrics, str):
        return 0
    # tokenize roughly by words
    tokens = re.findall(r"\b\w+\b", lyrics.lower())
    # count how many tokens are in the explicit vocabulary
    return sum(word in explicit_words for word in tokens)