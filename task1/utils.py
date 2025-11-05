import scipy.stats as stats
import pandas as pd
import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from typing import List, Tuple, Dict, Callable, Any
from pandas.api.types import CategoricalDtype

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

    mask_invalid = df[column].notna() & ~df[column].isin(valid_values)
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


def plot_categorical_distribution(
    series: pd.Series,
    *,
    figsize: tuple[float, float] = (6, 4),
    top_n: int | None = 10,
    title: str | None = None,
    palette: str | list[str] = "viridis",
    horizontal: bool = True,
    include_na: bool = True,
    order: list[str] | None = None,
    ax: Axes | None = None,
) -> plt.Figure:
   
    values = series.copy()
    if isinstance(values.dtype, CategoricalDtype):
        # ensure the placeholder category exists before assigning it
        if "Unknown" not in values.cat.categories:
            values = values.cat.add_categories(["Unknown"])

    if order is not None:
        mask_known = values.isin(order) | values.isna()
        values = values.where(mask_known, other="Unknown")

    if include_na:
        values = values.fillna("Unknown")
    else:
        values = values.dropna()

    counts = values.astype(str).value_counts()
    if order is not None:
        reindex_order = list(order)
        if "Unknown" in counts.index and "Unknown" not in reindex_order:
            reindex_order.append("Unknown")
        counts = counts.reindex(reindex_order, fill_value=0)
        if top_n is not None:
            counts = counts.iloc[:top_n]
    else:
        if top_n is not None:
            counts = counts.head(top_n)

    if horizontal and order is None:
        counts = counts.sort_values()

    plot_df = counts.rename_axis("category").reset_index(name="count")

    if ax is None:
        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(111)
    else:
        fig = ax.figure

    if plot_df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_xticks([])
        ax.set_yticks([])
    else:
        if horizontal:
            sns.barplot(
                data=plot_df,
                x="count",
                y="category",
                hue="category",
                ax=ax,
                orient="h",
                palette=palette,
                legend=False,
            )
            ax.set_xlabel("Count")
            ax.set_ylabel("")
        else:
            sns.barplot(
                data=plot_df,
                x="category",
                y="count",
                hue="category",
                ax=ax,
                palette=palette,
                legend=False,
            )
            ax.set_xlabel("")
            ax.set_ylabel("Count")
            ax.tick_params(axis="x", labelrotation=30)
            plt.setp(ax.get_xticklabels(), ha="right")

    ax.set_title(title or (series.name or ""))

    fig.tight_layout()
    return fig


def plot_numerical(
    series: pd.Series,
    *,
    title: str | None = None,
    bins: int = 30,
    log_scale: bool = False,
    kde: bool = True,
    color: str | None = None,
) -> plt.Figure:
   
    data = series.dropna()

    fig = plt.figure(figsize=(6, 4))
    ax = fig.add_subplot(111)

    if data.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_xticks([])
        ax.set_yticks([])
    else:
        sns.histplot(
            data,
            ax=ax,
            kde=kde,
            bins=bins,
            color=color or sns.color_palette("viridis", n_colors=1)[0],
            log_scale=log_scale,
        )
        ax.set_ylabel("Count")

    xlabel = series.name or "Value"
    if log_scale:
        xlabel = f"{xlabel} (log scale)"
    ax.set_xlabel(xlabel)
    ax.set_title(title or xlabel)

    fig.tight_layout()

    return fig


def plot_date_distribution(
    series: pd.Series,
    *,
    bins: int = 10,
    title: str | None = None,
    include_na_note: bool = False,
    ax: Axes | None = None,
    figsize: tuple[float, float] = (8, 4),
) -> plt.Figure:
   
    converted = pd.to_datetime(series, errors="coerce")
    valid_values = converted.dropna()
    missing_count = int(converted.isna().sum())

    if ax is None:
        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(111)
    else:
        fig = ax.figure
    if valid_values.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_xticks([])
        ax.set_yticks([])
    else:
        sns.histplot(valid_values, bins=bins, ax=ax)
        ax.set_xlabel(series.name or "Date")
        ax.set_ylabel("Count")

    plot_title = title or (series.name or "")
    if include_na_note and missing_count:
        plot_title = f"{plot_title} (unknown: {missing_count})"
    ax.set_title(plot_title)

    fig.tight_layout()

    return fig

import pandas as pd
import matplotlib.pyplot as plt

def plot_boxplot(df, column, by=None, title=None, figsize=(8, 6)):
    plt.figure(figsize=figsize)
    df.boxplot(column=column, by=by, grid=False, patch_artist=True,
               boxprops=dict(facecolor='lightblue', color='black'),
               medianprops=dict(color='red', linewidth=2),
               whiskerprops=dict(color='gray'),
               capprops=dict(color='gray'))
    plt.title(title if title else f'Boxplot of {column}')
    plt.suptitle('')  # Rimuove il titolo automatico di pandas
    plt.xlabel(by if by else '')
    plt.ylabel(column)
    plt.show()


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

language_codes = {
    'aa', 'bg', 'ca', 'chr', 'co', 'cs', 'cy', 'da', 'de', 'en', 'eo', 'es', 'et', 'eu', 'fr', 'gl', 'ia', 'it', 'la', 'lt', 'mt', 'nl', 'no', 'pl', 'pt', 'qu', 'rm', 'ro',
    'ru', 'rw', 'sco', 'sq', 'sr', 'sw', 'war',
}


# HELPER FUN: Drop multiple columns from a DataFrame if they exist
def drop_columns_if_exists(df, columns):
    existing_cols = [col for col in columns if col in df.columns]
    if existing_cols:
        df = df.drop(columns=existing_cols)
    return df

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


def scatterplot(df, x_col, y_col, figsize = (8,6), color_col=None, title=None, xlabel=None, ylabel=None):

    plt.figure(figsize=figsize)

    if color_col and color_col in df.columns:
        plt.scatter(df[x_col], df[y_col], c=df[color_col], cmap='viridis', s=40, alpha=0.7, edgecolors='k')
        plt.colorbar(label=color_col)
    else:
        plt.scatter(df[x_col], df[y_col], s=40, alpha=0.7, edgecolors='k')

    plt.title(title or f"{y_col} vs {x_col}", fontsize=14, fontweight='bold')
    plt.xlabel(xlabel or x_col, fontsize=12)
    plt.ylabel(ylabel or y_col, fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.show()
    

def one_hot_encode_array_feature(df, col_name):
    df = df.copy()
    
    uniques = sorted(set([item for sublist in df[col_name] for item in sublist]))

    for word in uniques:
        df[word] = df[col_name].apply(lambda x: 1 if word in x else 0)
        
    return df