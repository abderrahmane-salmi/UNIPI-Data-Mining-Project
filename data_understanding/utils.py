import scipy.stats as stats
import pandas as pd
import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
from typing import List, Tuple, Dict, Callable, Any

class DataQualityReporter():
    """
    Checks for missing and invalid values in a dataset.
    """
    def __init__(self, df: pd.DataFrame, feature_validator_functions: Dict[str, Callable[[pd.DataFrame, str], Any]] | None=None
                 , ignore_features: List[str] | None = None):
        self.df = df
        self.feature_validators = feature_validator_functions
        self.ignore_features = ignore_features
        self.report = {}

    def __getitem__(self, report_key):
        """Returns the corresponding report value"""
        if report_key not in self.report.keys():
            return None
        else:
            return self.report[report_key] if report_key in self.report else None
    
    def compute_report(self):
        # Adds features with missing values to report
        missing = self.df.isna().sum()
        self.report['missing_values'] = missing[missing > 0]

        features = self.df.columns.to_list()
        # Duplicate rows
        if self.ignore_features is None:
            self.report['duplicate_rows'] = self.df[self.df.duplicated()]
        else:
            features_to_consider = list(filter(lambda x: x not in self.ignore_features, features))
            self.report['duplicate_rows'] = self.df[self.df.duplicated(subset=features_to_consider)]    
        

        
        self.report['not_validated'] = []
        self.report["invalid"] = {}

        # Checks for invalid values in categorical features
        for feature in features:
            if feature not in self.feature_validators:
                # if the dictionary doesn't contain a function for the feature then it is not validated
                self.report['not_validated'].append(feature)
            else:
                invalid_values_function = self.feature_validators[feature]
                invalid = invalid_values_function(self.df)
                if invalid is not None:
                    self.report["invalid"][feature] = invalid
        return self.report
    
    def report_duplicated_rows(self):
        print(self.report["duplicate_rows"])
    
    def report_invalid_values(self):
        print(list(filter(lambda x: self.report['invalid'][x] != [],self.report['invalid'].keys())))


    def plot_missing_values(self):
        if self.report == {}:
            raise ValueError("Report is not computed")
        sns.heatmap(self.df.isnull(), cbar=False, annot=True, fmt="d", cmap="viridis") #soluzione momentanea, prima con self.report["missing_values"] non funzionava
        plt.title("Missing values in all_tracks.csv")
        plt.xlabel("Columns")
        plt.ylabel("")           # niente etichetta per la singola riga
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.show()



def check_in_set(df: pd.DataFrame, column:str, valid_values) -> List[Tuple[int, str]]:
    if column not in df.columns:
        raise ValueError(f"the column {column} doesn't exist in the dataframe")

    mask_invalid = ~df[column].isin(valid_values)
    return [(int(i), f"value '{df.loc[i, column]}' not in allowed set") for i in df.index[mask_invalid]]
    
def check_date(df: pd.DataFrame, column:str, date_min: str) -> List[Tuple[int, str]]:
    """assumes all values for the column have already been converted with pd.to_datetime()"""
    #copy_ = pd.to_datetime(df[column], errors="coerce")
    
    date_min = pd.to_datetime(date_min)
    date_max = pd.Timestamp.today()
    
    mask_too_old = (df[column] < date_min)
    mask_too_young = (df[column] > date_max)
    res = [(i,"too_old") for i in df.index[mask_too_old]]
    res.extend([(i,"too_young") for i in df.index[mask_too_young]])
    res.sort()
    return res

def check_numeric_range(df: pd.DataFrame, column: str, start: int|float, end:int|float):
    """assumes all numeric values for the column, it needs to have been checked before"""
    mask_too_small = (df[column] < start)
    mask_too_large = (df[column] > end)
    res = [(i, f"too small {column}") for i in df.index[mask_too_small]]
    res.extend([(i, f"too large {column}") for i in df.index[mask_too_large]])
    res.sort()
    return res


italian_regions = {
    "Piemonte",
    "Abruzzo",
    "Toscana",
    "Molise",
    "Emilia-Romagna",
    "Veneto",
    "Friuli-Venezia-Giulia",
    "Lombardia",
    "Valle d'Aosta",
    "Liguria",
    "Marche",
    "Lazio",
    "Umbria",
    "Campania",
    "Sardegna",
    "Sicilia",
    "Calabria",
    "Puglia",
    "Basilicata",
    "Trentino Alto Adige"
    }

mapping_province = {
    "Agrigento": "AG",
    "Alessandria": "AL",
    "Ancona": "AN",
    "Aosta": "AO",
    "Arezzo": "AR",
    "Ascoli Piceno": "AP",
    "Asti": "AT",
    "Avellino": "AV",
    "Bari": "BA",
    "Barletta-Andria-Trani": "BT",
    "Belluno": "BL",
    "Benevento": "BN",
    "Bergamo": "BG",
    "Biella": "BI",
    "Bologna": "BO",
    "Bolzano": "BZ",
    "Brescia": "BS",
    "Brindisi": "BR",
    "Cagliari": "CA",
    "Caltanissetta": "CL",
    "Campobasso": "CB",
    "Carbonia-Iglesias": "CI",
    "Caserta": "CE",
    "Catania": "CT",
    "Catanzaro": "CZ",
    "Chieti": "CH",
    "Como": "CO",
    "Cosenza": "CS",
    "Cremona": "CR",
    "Crotone": "KR",
    "Cuneo": "CN",
    "Enna": "EN",
    "Fermo": "FM",
    "Ferrara": "FE",
    "Firenze": "FI",
    "Foggia": "FG",
    "Forlì-Cesena": "FC",
    "Frosinone": "FR",
    "Genova": "GE",
    "Gorizia": "GO",
    "Grosseto": "GR",
    "Imperia": "IM",
    "Isernia": "IS",
    "La Spezia": "SP",
    "L'Aquila": "AQ",
    "Latina": "LT",
    "Lecce": "LE",
    "Lecco": "LC",
    "Livorno": "LI",
    "Lodi": "LO",
    "Lucca": "LU",
    "Macerata": "MC",
    "Mantova": "MN",
    "Massa-Carrara": "MS",
    "Matera": "MT",
    "Messina": "ME",
    "Milano": "MI",
    "Modena": "MO",
    "Monza e della Brianza": "MB",
    "Napoli": "NA",
    "Novara": "NO",
    "Nuoro": "NU",
    "Ogliastra": "OG",
    "Olbia-Tempio": "OT",
    "Oristano": "OR",
    "Padova": "PD",
    "Palermo": "PA",
    "Parma": "PR",
    "Pavia": "PV",
    "Perugia": "PG",
    "Pesaro e Urbino": "PU",
    "Pescara": "PE",
    "Piacenza": "PC",
    "Pisa": "PI",
    "Pistoia": "PT",
    "Pordenone": "PN",
    "Potenza": "PZ",
    "Prato": "PO",
    "Ragusa": "RG",
    "Ravenna": "RA",
    "Reggio Calabria": "RC",
    "Reggio Emilia": "RE",
    "Rieti": "RI",
    "Rimini": "RN",
    "Roma": "RM",
    "Rovigo": "RO",
    "Salerno": "SA",
    "Sassari": "SS",
    "Savona": "SV",
    "Siena": "SI",
    "Siracusa": "SR",
    "Sondrio": "SO",
    "Taranto": "TA",
    "Teramo": "TE",
    "Terni": "TR",
    "Torino": "TO",
    "Trapani": "TP",
    "Trento": "TN",
    "Treviso": "TV",
    "Trieste": "TS",
    "Udine": "UD",
    "Varese": "VA",
    "Venezia": "VE",
    "Verbano-Cusio-Ossola": "VB",
    "Vercelli": "VC",
    "Verona": "VR",
    "Vibo Valentia": "VV",
    "Vicenza": "VI",
    "Viterbo": "VT"
}



if __name__ == "__main__":
    df = pd.read_csv("../datasets/artists.csv", sep=";")
    feature_vectors = {
        "gender": lambda df: check_in_set(df, column="gender", valid_values={'M','F'}),
        "longitude": lambda df: check_numeric_range(df, column="longitude", start=-180, end=180),
        }
    dqr = DataQualityReporter(df, feature_validator_functions=feature_vectors)
    dqr.compute_report()
    dqr.report_duplicated_rows()
    dqr.plot_missing_values()
