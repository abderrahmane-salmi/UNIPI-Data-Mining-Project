import scipy.stats as stats
import pandas as pd
import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from typing import List, Tuple, Dict, Callable, Any
from pandas.api.types import CategoricalDtype
from sklearn.preprocessing import PowerTransformer
from IPython.display import display

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
        sns.heatmap(self.df.isnull(), cbar=False, annot=True, fmt="d", cmap="viridis")  # temporary solution; using self.report["missing_values"] did not work earlier
        plt.title("Missing values in all_tracks.csv")
        plt.xlabel("Columns")
        plt.ylabel("")           # no label for the single row
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

import pandas as pd
from typing import List, Tuple

def check_starting_year(
    df: pd.DataFrame,
    column: str = "year",
    artists_df: pd.DataFrame | None = None,
    artist_link_column: str = "id_artist",
    artist_id_column: str = "id",
    min_year: int = 1990,
    max_year: int = 2025,
) -> List[Tuple[int, str]]:
    
    res = []
    
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")
    
    raw_vals = df[column].copy()
    
    # --- FIX CRUCIALE: Logica di Conversione Robusta ---
    years = pd.Series(index=df.index, dtype=float)
    
    # Caso 1: È già una data (Datetime)
    if pd.api.types.is_datetime64_any_dtype(raw_vals):
        years = raw_vals.dt.year.astype(float)
        
    # Caso 2: È numerico o stringa che sembra un numero (es. "2020", 2020)
    # Proviamo PRIMA a convertirlo in numero puro. 
    # pd.to_numeric gestisce "2020" -> 2020.0 correttamente.
    else:
        numeric_conversion = pd.to_numeric(raw_vals, errors='coerce')
        
        # Se la conversione numerica ha funzionato per la maggior parte dei dati
        # e i valori sembrano anni (es. > 1900), usiamo quella.
        if numeric_conversion.notna().sum() > 0 and numeric_conversion.mean() > 1000:
             years = numeric_conversion.astype(float)
             
        # Caso 3: Fallback data (es. "2020-05-12")
        # Solo se non sembra un numero semplice, proviamo il parsing data
        else:
            as_datetime = pd.to_datetime(raw_vals, errors='coerce')
            years = as_datetime.dt.year.astype(float)

    # ----------------------------------------------------
    
    # 2. Check Absolute Bounds
    mask_too_old = years.notna() & (years < min_year)
    mask_too_new = years.notna() & (years > max_year)
    
    res.extend([(int(i), f"year {int(years.loc[i])} before {min_year}") for i in df.index[mask_too_old]])
    res.extend([(int(i), f"year {int(years.loc[i])} after {max_year}") for i in df.index[mask_too_new]])
    
    # 3. Check against Artist Info
    if artists_df is not None and artist_link_column in df.columns:
        
        # Helper interno semplificato
        def get_years_from_series(s):
            # Se è datetime
            if pd.api.types.is_datetime64_any_dtype(s):
                return s.dt.year.astype(float)
            # Altrimenti prova numero
            nums = pd.to_numeric(s, errors='coerce')
            if nums.mean() > 1000: # Euristica: se media > 1000 sono probabili anni
                return nums
            # Altrimenti prova data
            return pd.to_datetime(s, errors='coerce').dt.year.astype(float)

        # Preparazione dati artista
        artist_start_years = None
        artist_birth_years = None
        
        if 'active_start' in artists_df.columns:
            artist_start_years = get_years_from_series(
                artists_df.set_index(artist_id_column)['active_start']
            )
            
        if 'birth_date' in artists_df.columns:
            artist_birth_years = get_years_from_series(
                artists_df.set_index(artist_id_column)['birth_date']
            )

        # Loop di validazione
        for i in df.index:
            track_year = years.loc[i]
            artist_id = df.loc[i, artist_link_column]
            
            if pd.isna(track_year) or pd.isna(artist_id):
                continue
            
            # Check Active Start
            if artist_start_years is not None and artist_id in artist_start_years.index:
                artist_start = artist_start_years.loc[artist_id]
                # Tolleranza di 1 anno per differenze di release tra paesi
                if pd.notna(artist_start) and track_year < (artist_start - 1):
                    res.append((int(i), f"year {int(track_year)} before artist active_start {int(artist_start)}"))
            
            # Check Birth Date + 15
            if artist_birth_years is not None and artist_id in artist_birth_years.index:
                birth_year = artist_birth_years.loc[artist_id]
                if pd.notna(birth_year):
                    min_age_year = birth_year + 15
                    if track_year < min_age_year:
                        res.append((int(i), f"year {int(track_year)} implied artist age < 15 (born {int(birth_year)})"))
    
    res.sort()
    return res

def check_valid_lyrics(
    df: pd.DataFrame,
    lyrics_column: str = "lyrics",
    title_column: str = "title",
    min_length: int = 100,
) -> List[Tuple[int, str]]:
    """Check if lyrics are valid (not just title, reasonable length).
    
    Args:
        df: DataFrame with lyrics and title columns.
        lyrics_column: Name of the lyrics column.
        title_column: Name of the title column.
        min_length: Minimum character count for valid lyrics (default 100).
    
    Returns:
        List of (index, reason) tuples for invalid lyrics.
    """
    res = []
    for i in df.index:
        lyrics = df.loc[i, lyrics_column]
        title = df.loc[i, title_column] if title_column in df.columns else ""
        
        if pd.isna(lyrics) or not isinstance(lyrics, str):
            continue  # skip missing values
        
        lyrics_clean = lyrics.strip().lower()
        title_clean = str(title).strip().lower() if pd.notna(title) else ""
        
        # Check if lyrics are too short
        if len(lyrics_clean) < min_length:
            res.append((int(i), f"lyrics too short ({len(lyrics_clean)} chars, min {min_length})"))
        # Check if lyrics are just the title (or title repeated)
        elif title_clean and lyrics_clean.replace(title_clean, "").strip() == "":
            res.append((int(i), "lyrics contain only title"))
        elif title_clean and lyrics_clean == title_clean:
            res.append((int(i), "lyrics equal to title"))
    
    res.sort()
    return res


def detect_outliers_iqr(series: pd.Series, k: float = 1.5) -> pd.Series:
    """
    Flags values that fall outside the IQR fence (Q1 - k*IQR, Q3 + k*IQR).

    Parameters
    ----------
    series: pd.Series
        Numeric series to inspect. It is coerced to numeric in case of object dtypes.
    k: float
        IQR multiplier (1.5 gives the classical Tukey fence).

    Returns
    -------
    pd.Series
        Boolean mask aligned with the original index where True marks an outlier.
    """
    if not isinstance(series, pd.Series):
        raise TypeError("detect_outliers_iqr expects a pandas Series")

    numeric = pd.to_numeric(series, errors="coerce")
    q1 = numeric.quantile(0.25)
    q3 = numeric.quantile(0.75)
    iqr = q3 - q1

    if pd.isna(iqr) or iqr == 0:
        # If there is not enough spread we can't flag any point as an outlier
        return pd.Series(False, index=series.index, name=f"{series.name}_is_outlier")

    lower = q1 - k * iqr
    upper = q3 + k * iqr
    mask = (numeric < lower) | (numeric > upper)
    mask = mask.fillna(False)
    mask.name = f"{series.name}_is_outlier" if series.name else "is_outlier"
    return mask


def detect_outliers_iqr_with_score(series: pd.Series, k: float = 1.5) -> Tuple[pd.Series, pd.Series]:
    """
    Flags values that fall outside the IQR fence and returns an outlier score.
    Score is defined as distance from the nearest fence divided by IQR.
    0 means on the fence or inside. > 0 means outside.

    Parameters
    ----------
    series: pd.Series
        Numeric series to inspect.
    k: float
        IQR multiplier.

    Returns
    -------
    Tuple[pd.Series, pd.Series]
        (Boolean mask of outliers, Series of outlier scores)
    """
    if not isinstance(series, pd.Series):
        raise TypeError("detect_outliers_iqr_with_score expects a pandas Series")

    numeric = pd.to_numeric(series, errors="coerce")
    q1 = numeric.quantile(0.25)
    q3 = numeric.quantile(0.75)
    iqr = q3 - q1

    if pd.isna(iqr) or iqr == 0:
        return (
            pd.Series(False, index=series.index, name=f"{series.name}_is_outlier"),
            pd.Series(0.0, index=series.index, name=f"{series.name}_outlier_score")
        )

    lower = q1 - k * iqr
    upper = q3 + k * iqr

    # Calculate score: distance from fence / IQR
    # If x < lower: score = (lower - x) / IQR
    # If x > upper: score = (x - upper) / IQR
    # Else: score = 0
    
    scores = pd.Series(0.0, index=series.index, name=f"{series.name}_outlier_score")
    
    mask_lower = numeric < lower
    mask_upper = numeric > upper
    
    scores[mask_lower] = (lower - numeric[mask_lower]) / iqr
    scores[mask_upper] = (numeric[mask_upper] - upper) / iqr
    
    mask = mask_lower | mask_upper
    mask = mask.fillna(False)
    mask.name = f"{series.name}_is_outlier" if series.name else "is_outlier"
    
    return mask, scores


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
    ax: Axes | None = None,
    figsize: tuple[float, float] = (6, 4),
) -> plt.Figure:
    numeric = pd.to_numeric(series, errors="coerce").dropna()

    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.figure

    if numeric.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_xticks([])
        ax.set_yticks([])
    else:
        sns.histplot(
            numeric,
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


def plot_swear_words(df:pd.DataFrame, language: str ="IT", threshold: int = 20):
    col = f"swear_{language}_words"
    swear_cols = sorted(set([item for sublist in df[col] for item in sublist]))

    # Count occurrences (sum of the 1 values in each column)
    counts = df[swear_cols].sum().sort_values(ascending=False)

    counts = counts[counts>threshold]
    # Create the chart
    plt.figure(figsize=(10, 6))
    plt.bar(counts.index, counts.values, color="darkred", alpha=0.8)
    lang = "Italian" if language == "IT" else "English"
    plt.title(f"{lang} Swear Words Distribution", fontsize=14, fontweight="bold")
    plt.xlabel("Swear Word", fontsize=12)
    plt.ylabel("Occurences", fontsize=12)
    plt.xticks(rotation=45, ha="right")
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.show()


def plot_boxplot(
    series,
    by=None,
    title=None,
    figsize=(4, 6),
    outlier_series=None,
    ax: Axes | None = None,
):
    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
        created_fig = True
    else:
        fig = ax.figure

    # Handle case where series is a DataFrame and by is a column name
    if isinstance(series, pd.DataFrame) and isinstance(by, str) and by in series.columns:
        series = series[by]
        by = None

    sns.boxplot(
        x=by,
        y=series,
        ax=ax,
        color="lightblue",
        medianprops=dict(color="red", linewidth=2),
        boxprops=dict(edgecolor="black"),
        whiskerprops=dict(color="gray"),
        capprops=dict(color="gray"),
    )

    if outlier_series is not None:
        mask_outliers = outlier_series.astype(bool)
        if by is None:
            ax.scatter(
                np.zeros(mask_outliers.sum()),
                series[mask_outliers],
                color="tomato",
                edgecolors="k",
                alpha=0.9,
                label="Outlier",
            )
        else:
            # map group labels to x positions
            tick_labels = [t.get_text() for t in ax.get_xticklabels()]
            pos_map = {label: pos for pos, label in enumerate(tick_labels)}
            
            # Create a temporary dataframe to handle grouping
            # Ensure series and by are aligned
            if isinstance(series, pd.Series) and isinstance(by, pd.Series):
                 temp_df = pd.DataFrame({'val': series, 'grp': by})
                 # We need to align outlier_series as well
                 if isinstance(outlier_series, pd.Series):
                     temp_df['out'] = outlier_series
                 else:
                     temp_df['out'] = outlier_series
            else:
                 # Fallback if not series (e.g. arrays)
                 temp_df = pd.DataFrame({'val': series, 'grp': by, 'out': mask_outliers})

            for label, grp in temp_df.groupby('grp'):
                grp_out = grp[grp['out'].astype(bool)]
                if grp_out.empty:
                    continue
                x_pos = pos_map.get(str(label))
                if x_pos is None:
                    continue
                ax.scatter(
                    np.full(len(grp_out), x_pos),
                    grp_out['val'],
                    color="tomato",
                    edgecolors="k",
                    alpha=0.9,
                    label="Outlier" if "Outlier" not in ax.get_legend_handles_labels()[1] else None,
                )
        handles, labels = ax.get_legend_handles_labels()
        handles_labels = [(h, l) for h, l in zip(handles, labels) if l]
        if handles_labels:
            handles, labels = zip(*handles_labels)
            ax.legend(handles, labels)

    ax.set_title(title if title else f"Boxplot of {series.name if hasattr(series, 'name') else ''}")
    ax.set_xlabel(by.name if hasattr(by, 'name') and by.name else "")
    ax.set_ylabel(series.name if hasattr(series, 'name') else "")
    fig.suptitle("")
    fig.tight_layout()
    if created_fig:
        plt.show()
    return fig


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


def scatterplot(df, x_col, y_col, figsize=(8,6), color_col=None, outlier_col=None, title=None, xlabel=None, ylabel=None):

    plt.figure(figsize=figsize)

    # If an outlier flag is provided, draw inliers/outliers with different colors
    if outlier_col and outlier_col in df.columns:
        mask_outliers = df[outlier_col].astype(bool)
        plt.scatter(
            df.loc[~mask_outliers, x_col],
            df.loc[~mask_outliers, y_col],
            s=40,
            alpha=0.7,
            edgecolors='k',
            label='Inlier',
            color='tab:blue',
        )
        plt.scatter(
            df.loc[mask_outliers, x_col],
            df.loc[mask_outliers, y_col],
            s=50,
            alpha=0.9,
            edgecolors='k',
            label='Outlier',
            color='tomato',
        )
        plt.legend(title=outlier_col)
    elif color_col and color_col in df.columns:
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

    # Build a dictionary of binary columns
    one_hot_dict = {
        word: df[col_name].apply(lambda x: int(word in x))
        for word in uniques
    }

    # Create a DataFrame from it
    one_hot_df = pd.DataFrame(one_hot_dict, index=df.index)
    df = pd.concat([df, one_hot_df], axis=1)
    

    return df

def get_yeo_johnson(series):
    """Apply Yeo-Johnson transformation."""
    pt = PowerTransformer(method='yeo-johnson')
    return pd.Series(pt.fit_transform(series.values.reshape(-1, 1)).flatten(), index=series.index)


default_transforms = {
        'default': lambda x: x,
        'log': lambda x: np.log1p(x if x.min() > 0 else x + abs(x.min()) + 1),
        'sqrt': lambda x: np.sqrt(x if x.min() >= 0 else x + abs(x.min()) + 1),
        'yeo-johnson': get_yeo_johnson
    }

    
def analyze_feature(
    df: pd.DataFrame,
    feature: str,
    transforms: Dict[str, Callable[[pd.Series], pd.Series]]=default_transforms,
    outlier_function: Callable[[pd.Series], pd.Series]=detect_outliers_iqr,
    figsize: Tuple[int, int]=(12, 16),
    colors: List[str]=['skyblue', 'lightgreen', 'salmon', 'orange'],
    shift_threshold: float = 500
    ) -> Dict[str, Dict[str, Any]]:
    """
    Analyze a feature with multiple transformations and visualizations.
    
    If all values are > shift_threshold, the data is shifted to start from 0
    to avoid numerical issues with transformations like Yeo-Johnson.
    """

    series = df[feature].dropna()
    
    # Shift data if minimum is above threshold (e.g., years like 1990-2025)
    shift_applied = 0
    if series.min() > shift_threshold:
        shift_applied = series.min()
        series = series - shift_applied
        print(f" Data shifted by -{shift_applied} (original min > {shift_threshold})")
    
    results = {} 
    
    fig, axes = plt.subplots(len(transforms.keys()), 2, figsize=figsize)
    fig.suptitle(f'Analisi Distribuzione & Outliers: {feature}' + 
                 (f' (shifted by -{shift_applied})' if shift_applied else ''), 
                 fontsize=16, y=1.02)
    
    for i, (name, transform) in enumerate(transforms.items()):
        data = transform(series)
        skew = data.skew()
        kurt = data.kurtosis()
        
        outliers_mask = outlier_function(data)
        num_outliers = outliers_mask.sum()
        
        results[name] = {
            'distribution': data,
            'outliers_mask': outliers_mask, 
            'num_outliers': num_outliers,
            'stats': {'skew': skew, 'kurt': kurt},
            'shift_applied': shift_applied
        }
        
        color = colors[i]
        
        sns.histplot(data, kde=True, ax=axes[i, 0], color=color)
        axes[i, 0].set_title(f'{name} | Skew: {skew:.2f} | Kurt: {kurt:.2f}')
        axes[i, 0].set_xlabel('')
        
        sns.boxplot(x=data, ax=axes[i, 1], color=color, orient='h', 
                    flierprops={"marker": "o", "markerfacecolor": "red", "markersize": 6})
        axes[i, 1].set_title(f'{name} | Outliers found: {num_outliers}')
        axes[i, 1].set_xlabel('')

    plt.tight_layout()
    plt.show()
    
    return results



def apply_transforms(
    df: pd.DataFrame,
    feature_transforms: Dict[str, Callable[[pd.Series], pd.Series]],
    inplace: bool = False,
) -> pd.DataFrame:
    """Apply transformations to multiple features in a DataFrame.
    
    Args:
        df: Input DataFrame.
        feature_transforms: Dictionary mapping column names to transform functions.
                           Each function takes a Series and returns a transformed Series.
        inplace: If True, modify df in place. Otherwise return a copy.
    
    Returns:
        DataFrame with transformed features.
    
    Example:
        >>> transforms = {
        ...     'duration_ms': lambda s: np.log1p(s),
        ...     'popularity': lambda s: np.sqrt(s),
        ... }
        >>> df_transformed = apply_transforms(df, transforms)
    """
    result = df if inplace else df.copy()
    
    for feature, transform in feature_transforms.items():
        if feature not in result.columns:
            print(f"Warning: Column '{feature}' not found in DataFrame, skipping.")
            continue
        try:
            result[feature] = transform(result[feature])
        except Exception as e:
            print(f"Error transforming '{feature}': {e}")
    
    return result

def apply_winsorization(analysis_res: dict, transform_name: str) -> pd.Series:
    """
    Applica winsorization usando i limiti già calcolati in analyze_feature.
    Schiaccia gli outliers sui valori min/max degli inliers.
    """
    # 1. Estrai i dati calcolati
    data_info = analysis_res[transform_name]
    series = data_info['distribution']
    is_outlier = data_info['outliers_mask']
    
    # 2. Trova i limiti sicuri (min e max dei dati NON outlier)
    # ~is_outlier seleziona solo le righe False (inliers)
    inliers = series[~is_outlier]
    
    if inliers.empty:
        return series # Sicurezza se tutto è outlier
        
    lower_limit = inliers.min()
    upper_limit = inliers.max()
    
    # 3. Winsorization (Clip)
    # Tutti i valori sotto lower_limit diventano lower_limit
    # Tutti i valori sopra upper_limit diventano upper_limit
    print(f"Winsorizing '{transform_name}': clipped to range [{lower_limit:.4f}, {upper_limit:.4f}]")
    return series.clip(lower=lower_limit, upper=upper_limit)