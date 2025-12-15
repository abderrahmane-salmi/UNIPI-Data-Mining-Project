import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

def plot_timeseries_clusters(X, labels, centroids=None, feature_idx=0, title="Cluster Analysis", figsize=(12, 10)):
    """
    Plots time series grouped by clusters. Handles dynamic cluster counts and DBSCAN noise.
    """
    unique_labels = sorted(np.unique(labels))
    n_clusters = len(unique_labels)
    cmap = plt.get_cmap('tab10')
    
    # Setup grid
    fig, axes = plt.subplots(nrows=n_clusters, ncols=1, figsize=figsize, sharex=True, constrained_layout=True)
    if n_clusters == 1: axes = [axes]
    axes = np.array(axes).flatten()

    for i, label in enumerate(unique_labels):
        ax = axes[i]
        
        # Select data for current cluster
        mask = labels == label
        cluster_data = X[mask, :, feature_idx]
        
        # Handle colors and naming (DBSCAN noise = -1)
        is_noise = (label == -1)
        color = 'gray' if is_noise else cmap(i % 10)
        cluster_name = "Noise (-1)" if is_noise else f"Cluster {label}"

        # 1. Plot individual time series
        ax.plot(cluster_data.T, color=color, alpha=0.15, linewidth=0.5)

        # 2. Plot Centroid or Mean
        if centroids is not None and not is_noise:
            center = centroids[label, :, feature_idx]
            ax.plot(center, color='black', linewidth=2, linestyle='--', label='Centroid')
        elif len(cluster_data) > 0:
            mean_trend = np.mean(cluster_data, axis=0)
            style = ':' if is_noise else '--'
            ax.plot(mean_trend, color='black', linewidth=2, linestyle=style, label='Mean')

        # Formatting
        ax.set_title(f"{cluster_name} (n={len(cluster_data)})")
        ax.set_ylabel(f"Amplitude (Feat {feature_idx})")
        ax.legend(loc='upper right')
        ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel("Time Steps")
    plt.suptitle(title, fontsize=16)
    plt.show()


def plot_cluster_purity(y_true, y_pred, title="Cluster Purity: Ground Truth vs Clusters", cmap='Blues', figsize=(6, 5)):
    """
    Genera una heatmap (confusion matrix) per confrontare le etichette reali con i cluster trovati.
    
    Args:
        y_true (list/array): Le etichette reali (es. ['Fedez', 'Fibra', ...]).
        y_pred (list/array): Le etichette dei cluster (es. [0, 1, 0, -1...]).
        title (str): Titolo del grafico.
        cmap (str): Colormap di Seaborn (es. 'Blues', 'Greens', 'Reds').
        figsize (tuple): Dimensioni della figura.
        
    Returns:
        pd.DataFrame: La tabella incrociata (crosstab) calcolata.
    """
    
    # 1. Creazione della Tabella Incrociata
    # Usiamo pd.Series con 'name' per avere etichette automatiche sugli assi
    crosstab = pd.crosstab(
        pd.Series(y_true, name='Artista Reale (Ground Truth)'), 
        pd.Series(y_pred, name='Cluster Predetto (Model)')
    )
    
    # 2. Visualizzazione
    plt.figure(figsize=figsize)
    
    # annot=True scrive i numeri nelle celle
    # fmt='d' assicura che siano numeri interi (decimal)
    sns.heatmap(crosstab, annot=True, fmt='d', cmap=cmap, cbar=False, square=True)
    
    plt.title(title, fontsize=14)
    plt.tight_layout()
    plt.show()
    
    return crosstab