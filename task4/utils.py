import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

def plot_timeseries_clusters(X, labels, centroids=None, feature_idx=0, title="Cluster Analysis", figsize=(12, 12)):
    """
    Plots time series grouped by clusters + a final comparison plot of all centroids.
    """
    unique_labels = sorted(np.unique(labels))
    n_clusters = len(unique_labels)
    cmap = plt.get_cmap('tab10')
    
    fig, axes = plt.subplots(nrows=n_clusters + 1, ncols=1, figsize=figsize, sharex=True, constrained_layout=True)
    if not isinstance(axes, np.ndarray): axes = [axes]
    axes = np.ravel(axes) # Assicura che sia sempre 1D array

    ax_summary = axes[-1]
    ax_summary.set_title("Centroids Comparison")
    ax_summary.grid(True, alpha=0.3)

    for i, label in enumerate(unique_labels):
        ax = axes[i]
        mask = labels == label
        cluster_data = X[mask, :, feature_idx]

        is_noise = (label == -1)
        color = 'gray' if is_noise else cmap(i % 10)
        cluster_name = "Noise (-1)" if is_noise else f"Cluster {label}"

        if len(cluster_data) > 0:
            ax.plot(cluster_data.T, color=color, alpha=0.15, linewidth=0.5)

        representative_line = None
        
        if centroids is not None and not is_noise:
            representative_line = centroids[label, :, feature_idx]
            ax.plot(representative_line, color='black', linewidth=2, linestyle='--', label='Centroid')
            
        elif len(cluster_data) > 0:
            representative_line = np.mean(cluster_data, axis=0)
            style = ':' if is_noise else '--'
            col_line = 'black' if not is_noise else 'red' # Rumore medio in rosso per distinguerlo
            ax.plot(representative_line, color=col_line, linewidth=2, linestyle=style, label='Mean')

        ax.set_title(f"{cluster_name} (n={len(cluster_data)})")
        ax.set_ylabel(f"Amp (Feat {feature_idx})")
        ax.legend(loc='upper right')
        ax.grid(True, alpha=0.3)

        if representative_line is not None:
            lbl_sum = f"Cluster {label}" if not is_noise else "Noise Avg"
            style_sum = '-' if not is_noise else ':'
            width_sum = 2 if not is_noise else 1
            
            ax_summary.plot(representative_line, 
                            color=color, 
                            linewidth=width_sum, 
                            linestyle=style_sum, 
                            label=lbl_sum)

    # Formatting Summary Plot
    ax_summary.set_xlabel("Time Steps")
    ax_summary.set_ylabel(f"Amplitude (Feat {feature_idx})")
    ax_summary.legend(loc='upper right')

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