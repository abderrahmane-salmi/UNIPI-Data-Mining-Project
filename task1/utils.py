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



def analyze_feature_comprehensive(df, feature, bounds=None):
    """
    Analyze a feature with multiple transformations and visualizations.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Input dataframe
    feature : str
        Name of the feature to analyze
    bounds : tuple, optional
        (min, max) tuple for domain validity check
    """
    print(f"--- Comprehensive Analysis for '{feature}' ---")
    series = df[feature].dropna()
    
    # 1. Domain Check
    if bounds:
        min_bound, max_bound = bounds
        invalid = df[(df[feature] <= min_bound) | (df[feature] > max_bound)]
        if len(invalid) > 0:
            print(f"\nPotential Data Errors ({feature} <= {min_bound} or > {max_bound}): {len(invalid)}")
            cols_to_show = ['artist_name', 'track_name', feature] if 'artist_name' in df.columns else [feature]
            if 'name_artist' in df.columns: # Handle alternative column names
                 cols_to_show = ['name_artist', 'title', feature]
            print(invalid[cols_to_show].head())
    
    # Prepare Transformations
    # Original
    outliers_orig = detect_outliers_iqr(series)
    skew_orig = series.skew()
    kurt_orig = series.kurtosis()
    
    # Log1p
    min_val = series.min()
    series_shifted = series - min_val if min_val < 0 else series
    log_series = np.log1p(series_shifted)
    outliers_log = detect_outliers_iqr(log_series)
    skew_log = log_series.skew()
    kurt_log = log_series.kurtosis()
    
    # Sqrt
    sqrt_series = np.sqrt(series_shifted)
    outliers_sqrt = detect_outliers_iqr(sqrt_series)
    skew_sqrt = sqrt_series.skew()
    kurt_sqrt = sqrt_series.kurtosis()
    
    # Yeo-Johnson
    yj_series = get_yeo_johnson(series)
    outliers_yj = detect_outliers_iqr(yj_series)
    skew_yj = yj_series.skew()
    kurt_yj = yj_series.kurtosis()
    
    # 2. Visualization
    fig, axes = plt.subplots(4, 2, figsize=(10, 15))
    fig.suptitle(f'Feature Analysis: {feature}', fontsize=16)
    
    methods = [
        ('Original', series, outliers_orig, skew_orig, 'skyblue'),
        ('Log1p', log_series, outliers_log, skew_log, 'lightgreen'),
        ('Sqrt', sqrt_series, outliers_sqrt, skew_sqrt, 'salmon'),
        ('Yeo-Johnson', yj_series, outliers_yj, skew_yj, 'orange')
    ]
    
    for i, (name, data, outliers, skew, color) in enumerate(methods):
        # Distribution Plot (Left)
        sns.histplot(data, kde=True, ax=axes[i, 0], color=color)
        axes[i, 0].set_title(f'{name} Distribution (Skew: {skew:.2f})')
        
        # Horizontal Boxplot (Right)
        sns.boxplot(x=data, ax=axes[i, 1], color=color, orient='h')
        axes[i, 1].set_title(f'{name} Boxplot (Outliers: {outliers.sum()})')
        
        # Highlight outliers on boxplot (optional, but boxplot does it natively)
        # We can add strip plot for better visibility if needed, but boxplot is usually enough
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.show()

    return {
        'Orig Outliers': outliers_orig.sum(), 'Orig Skew': skew_orig, 'Orig Kurt': kurt_orig,
        'Log Outliers': outliers_log.sum(), 'Log Skew': skew_log, 'Log Kurt': kurt_log,
        'Sqrt Outliers': outliers_sqrt.sum(), 'Sqrt Skew': skew_sqrt, 'Sqrt Kurt': kurt_sqrt,
        'YJ Outliers': outliers_yj.sum(), 'YJ Skew': skew_yj, 'YJ Kurt': kurt_yj
    }


default_transforms = {
    'Default': lambda series: series,
    'Log': lambda series: np.log1p(series if series.min() > 0 else series + abs(series.min()) + 1),
    'Sqrt': lambda series: np.sqrt(series if series.min() >= 0 else series + abs(series.min()) + 1),
    'Yeo-Johnson': lambda series: pd.Series(stats.yeojohnson(series)[0], index=series.index)
}
    
def analyze_feature(
    df: pd.DataFrame,
    feature: str,
    transforms: Dict[str, Callable[[pd.Series], pd.Series]]=default_transforms,
    outlier_function: Callable[[pd.Series], pd.Series]=detect_outliers_iqr,
    figsize: Tuple[int, int]=(12, 16),
    colors: List[str]=['skyblue', 'lightgreen', 'salmon', 'orange']
    ) -> Dict[str, Dict[str, Any]]:

    series = df[feature].dropna()
    
    results = {} 
    
    fig, axes = plt.subplots(len(transforms.keys()), 2, figsize=figsize)
    fig.suptitle(f'Analisi Distribuzione & Outliers: {feature}', fontsize=16, y=1.02)
    
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
            'stats': {'skew': skew, 'kurt': kurt}
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


def process_outlier_results(df, feature, results):
    """
    Process results from analyze_feature_comprehensive to determine best transformation
    and detect outliers.
    """
    if results is None:
        return
        
    # Re-structuring the result for DataFrame display
    methods = ['Original', 'Log1p', 'Sqrt', 'Yeo-Johnson']
    # Ensure values are extracted correctly
    outliers = [
        results['Orig Outliers'],
        results['Log Outliers'],
        results['Sqrt Outliers'],
        results['YJ Outliers']
    ]
    skews = [
        results['Orig Skew'],
        results['Log Skew'],
        results['Sqrt Skew'],
        results['YJ Skew']
    ]
    kurts = [
        results['Orig Kurt'],
        results['Log Kurt'],
        results['Sqrt Kurt'],
        results['YJ Kurt']
    ]
    
    # Handle NaNs by replacing them with infinity so they are not selected
    scores = []
    for s, k in zip(skews, kurts):
        if pd.isna(s) or pd.isna(k):
            scores.append(np.inf)
        else:
            scores.append(abs(s) + abs(k))
    
    results_df = pd.DataFrame({
        'Method': methods,
        'Outliers': outliers,
        'Skewness': skews,
        'Kurtosis': kurts,
        'Score': scores
    })
    
    display(results_df)
    
    # Identify best transformation (lowest Score)
    # If all scores are inf (e.g. all NaNs), default to Original
    if results_df['Score'].min() == np.inf:
        print(f"Warning: Could not calculate valid scores for {feature}. Defaulting to Original.")
        best_method = 'Original'
        best_score = np.nan
    else:
        best_method_row = results_df.loc[results_df['Score'].idxmin()]
        best_method = best_method_row['Method']
        best_score = best_method_row['Score']
    
    print(f"Best transformation for {feature}: {best_method} (Score: {best_score:.4f})")
    
    # Apply best transformation to generate outlier mask (Detection Only)
    series = df[feature].dropna()
    if series.empty:
        print(f"No data for {feature} to detect outliers.")
        univariate_outlier_masks[feature] = pd.Series(False, index=df.index, dtype=bool)
        return

    if best_method == 'Log1p':
        min_val = series.min()
        series_shifted = series - min_val if min_val < 0 else series
        transformed = np.log1p(series_shifted)
    elif best_method == 'Sqrt':
        min_val = series.min()
        series_shifted = series - min_val if min_val < 0 else series
        transformed = np.sqrt(series_shifted)
    elif best_method == 'Yeo-Johnson':
        transformed = get_yeo_johnson(series)
    else:
        transformed = series
        
    outlier_mask, outlier_scores = detect_outliers_iqr_with_score(transformed)
    outlier_count = outlier_mask.sum()
    
    # Store the mask
    # Initialize full mask as False, explicitly numpy bool
    full_mask = pd.Series(False, index=df.index, dtype=bool)
    full_scores = pd.Series(0.0, index=df.index, dtype=float)
    
    # Set True only for detected outliers
    # outlier_mask is a boolean Series on the subset (non-nulls).
    # We get indices where it is True.
    if outlier_count > 0:
        outlier_indices = outlier_mask[outlier_mask].index
        # Ensure indices are valid for full_mask
        valid_indices = outlier_indices.intersection(full_mask.index)
        full_mask.loc[valid_indices] = True
        
        # Store scores
        full_scores.loc[transformed.index] = outlier_scores
    
    if outlier_count > 0:
        # Update global outlier columns if they exist
        if 'outlier' in df.columns:
             df['outlier'] = df['outlier'] | full_mask
        if 'outlier_score' in df.columns:
             df['outlier_score'] = df['outlier_score'].where(~full_mask, np.maximum(df['outlier_score'], full_scores))

        print(f"Detected {outlier_count} outliers for {feature} using {best_method} transformation (Mask stored).")
    else:
        print(f"No outliers detected for {feature} using {best_method} transformation.")
    
     # 3. Inspect Top Outliers (Original)
    if outliers_orig.sum() > 0:
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        
        outliers_df = df.loc[series.index][(series < lower) | (series > upper)]
        
        print(f"\nOriginal IQR Bounds: {lower:.2f} - {upper:.2f}")
        print(f"Total Outliers (Original): {len(outliers_df)}")
        
        cols_to_show = ['artist_name', 'track_name', feature] if 'artist_name' in df.columns else [feature]
        if 'name_artist' in df.columns:
             cols_to_show = ['name_artist', 'title', feature]
             
        print("\nTop 5 Low Outliers:")
        print(outliers_df.sort_values(feature).head(5)[cols_to_show])
        print("\nTop 5 High Outliers:")
        print(outliers_df.sort_values(feature, ascending=False).head(5)[cols_to_show])
