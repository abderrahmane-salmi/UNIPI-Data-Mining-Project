import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

from utils import mapping_province


REPO_ROOT = Path(__file__).resolve().parent.parent
DATASETS_DIR = REPO_ROOT / "datasets"


def load_artists(path: str | None = None) -> pd.DataFrame:
    """
    Loads artists dataset using the proper separator and Python engine to
    gracefully handle textual fields that may contain the separator.
    """
    csv_path = DATASETS_DIR / "artists.csv" if path is None else Path(path)
    return pd.read_csv(csv_path, sep=";", engine="python")


def load_tracks(path: str | None = None) -> pd.DataFrame:
    """
    Loads tracks dataset with the Python engine so that multi-line lyrics
    are parsed correctly.
    """
    csv_path = DATASETS_DIR / "tracks.csv" if path is None else Path(path)
    return pd.read_csv(csv_path, engine="python")


def plot_top_categories(
    df: pd.DataFrame,
    column: str,
    ax: plt.Axes,
    *,
    top_n: int = 15,
    title: str | None = None,
) -> None:
    """
    Plots the top N categories for a given column as a horizontal bar chart.
    """
    counts = (
        df[column]
        .value_counts(dropna=False)
        .head(top_n)
        .sort_values(ascending=True)
    )
    plot_df = counts.reset_index()
    plot_df.columns = [column, "count"]
    plot_df[column] = plot_df[column].astype(str)

    sns.barplot(
        data=plot_df,
        x="count",
        y=column,
        orient="h",
        ax=ax,
        palette="viridis",
        hue=column,
        legend=False,
    )
    ax.set_title(title or column)
    ax.set_xlabel("Frequency")
    ax.set_ylabel("")


def plot_artists_features(df: pd.DataFrame) -> plt.Figure:
    """
    Builds the figure for the two key artist features: genre and province.
    """
    artists = df.copy()
    artists["province_abbrev"] = artists["province"].map(mapping_province)
    artists["province_abbrev"] = artists["province_abbrev"].fillna(artists["province"])

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    plot_top_categories(artists, "gender", axes[0], top_n=5, title="Artists by gender")
    plot_top_categories(
        artists,
        "province_abbrev",
        axes[1],
        top_n=15,
        title="Artists by province (top 15)",
    )
    fig.tight_layout()
    return fig


def plot_tracks_features(df: pd.DataFrame) -> plt.Figure:
    """
    Builds the figure for the two key track features: language and explicit flag.
    """
    tracks = df.copy()

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    plot_top_categories(
        tracks,
        "language",
        axes[0],
        top_n=10,
        title="Tracks by language (top 10)",
    )
    plot_top_categories(
        tracks,
        "explicit",
        axes[1],
        top_n=3,
        title="Tracks explicit vs clean",
    )
    fig.tight_layout()
    return fig


def main() -> None:
    sns.set_theme(style="whitegrid")

    artists_df = load_artists()
    tracks_df = load_tracks()

    artists_fig = plot_artists_features(artists_df)
    tracks_fig = plot_tracks_features(tracks_df)

    artists_fig.suptitle("Artists dataset", fontsize=16, y=1.02)
    tracks_fig.suptitle("Tracks dataset", fontsize=16, y=1.02)

    plt.show()


if __name__ == "__main__":
    main()
