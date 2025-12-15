city_class_map = {
    'roma' : 'Lazio',
    'trastevere':'Lazio',
    'gianicolo':'Lazio',
    'tevere':'Lazio',
    'milano' : 'Lombardia',
    'lambrate':'Lombardia',
    'calvairate':'Lombardia',
    'rozzi':'Lombardia',
    'rozzano':'Lombardia',
    'cinisello':'Lombardia',
    'bologna' : 'Center',
    'bolo': 'Center',
    'napoli':'Campania',
    'napule':'Campania',
    'secondigliano':'Campania',
    'scampia':'Campania',
    'spaccanapoli':'Campania',
    'salerno':'Campania',
    'caserta':'Campania',
    'palapartenope': 'Campania',
    'partenope':'Campania',
    'torino': 'North',
    'genova':'North',
    'liguria':'North'
}

dialect_class_map ={
    'pischella':'Lazio',
    'pischello':'Lazio',
    'pischelli':'Lazio',
    'annamo':'Lazio',
    'ao':'Lazio',
    'aò':'Lazio',
    '\'nnamo':'Lazio',
    'annamo':'Lazio',
    'mortacci':'Lazio',
    'coatto':'Lazio',
    'coatta':'Lazio',
    'der':'Lazio',

    'guaglione':'Campania',
    'ngopp':'Campania',
    'ncopp':'Campania',
    'acopp':'Campania',
    'nisciun':'Campania',
    'nisciuno':'Campania',
    'stongo':'Campania',
    'stong':'Campania',
    'rind':'Campania',
    'chillu':'Campania',
    'munno':'Campania',
    'miett':'Campania',
    'criaturo':'Campania',
    'criatur':'Campania',
    'criatura':'Campania',
    'chiagne':'Campania',
    'chiagnere':'Campania',
    'guagliona':'Campania', 
    'sang':'Campania',
    'nient':'Campania',
    'miezz':'Campania',
    'amm\'':'Campania',
    'nuje':'Campania',
    'simmo':'Campania',
    'cchiù':'Campania', 
    'pecché':'Campania',
    'toja':'Campania',
    'agg':'Campania',
    'aggio':'Campania',
    'aggia':'Campania',
    'ditto':'Campania', 
    'chella':'Campania',
    'faje':'Campania',
    'ammore':'Campania'
}


def detect_region_references(tokens, threshold=0):
    classes = ['Lombardia','Lazio','Campania','North','Center']
    points = {class_:0 for class_ in classes}

    dialect_keys = dialect_class_map.keys()
    city_keys = city_class_map.keys()
    for token in tokens:
        if token in dialect_keys:
            points[dialect_class_map[token]]+=1
        elif token in city_keys:
            points[city_class_map[token]]+=1

    if max(points[region] for region in classes) <= threshold:
        return 'nan'
    return max(points, key=points.get)

if __name__=='__main__':
    test = ['milano','milano','milano','roma']
    test_dialect=['je','je','je','je','guaglione','guaglione','guaglione','ngopp']
    t3 = test + test_dialect
    print(detect_region_references(t3))
    