import copy
import math
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset, Subset, WeightedRandomSampler
from sklearn.metrics import f1_score
from sklearn.model_selection import StratifiedKFold
import matplotlib.pyplot as plt
import numpy as np
import librosa
from sklearn.model_selection import StratifiedKFold, PredefinedSplit, train_test_split
from tslearn.preprocessing import TimeSeriesScalerMeanVariance
from tslearn.utils import to_time_series_dataset
from typing import Dict, List, Callable, Tuple
# Local Imports
try:
    from SoftShape_repo.models.SoftShapeModel import SoftShapeNet
except ModuleNotFoundError:
    try:
        from task4.SoftShape_repo.models.SoftShapeModel import SoftShapeNet
    except ModuleNotFoundError:
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), 'SoftShape_repo'))
        from models.SoftShapeModel import SoftShapeNet

# =============================================================================
# 1. DATASET & DATA LOADING
# =============================================================================

class ShapeletDataset(Dataset):
    """Simple PyTorch Dataset for Shapelet learning."""
    def __init__(self, X, y):
        self.X = torch.tensor(X, dtype=torch.float32)
        self.y = torch.tensor(y, dtype=torch.long)
        
    def __len__(self):
        return len(self.X)
        
    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]

def plot_pyts_shapelets(transformer, X_train, n_top=5):
    """
    Visualizes the top shapelets extracted by pyts superimposed on the original time series.
    
    Args:
        transformer: Fitted pyts ShapeletTransform object.
        X_train: Training data (N, L).
        n_top: Number of top shapelets to visualize.
    """
    import matplotlib.pyplot as plt
    try:
        shapelets = transformer.shapelets_
        indices = transformer.indices_ # Indices (sample_id, start_time, end_time)
    except AttributeError:
        print("Transformer does not have shapelets_ or indices_ attributes. Make sure it is fitted.")
        return

    # Take top 'n_top' shapelets
    n_plot = min(n_top, len(shapelets))
    
    fig, axes = plt.subplots(n_plot, 1, figsize=(12, 3 * n_plot))
    if n_plot == 1: axes = [axes] # Ensure iterable

    for i in range(n_plot):
        s = shapelets[i]
        
        # Get context (where it was extracted from)
        sample_idx, start, end = indices[i]
        
        # Handle 3D input if necessary
        if X_train.ndim == 3:
            original_ts = X_train[sample_idx, :, 0]
        else:
            original_ts = X_train[sample_idx]
        
        ax = axes[i]
        # Plot original time series in gray
        ax.plot(original_ts, color='lightgray', label='Original Time Series', alpha=0.7)
        # Plot shapelet in correct position
        ax.plot(range(start, end), s, color='red', lw=2, label=f'Shapelet {i+1}')
        
        ax.set_title(f"Shapelet {i+1} (Length: {len(s)}) extracted from sample {sample_idx}")
        ax.legend(loc='upper right')
        ax.grid(alpha=0.3)
    
    plt.tight_layout()
    plt.show()

def _create_sampler(y_data, verbose=False):
    """
    Creates a WeightedRandomSampler to handle class imbalance.
    
    Args:
        y_data (np.ndarray): Labels array.
        verbose (bool): Whether to print warnings.
        
    Returns:
        WeightedRandomSampler or None: The sampler if valid, else None.
    """
    class_counts = np.bincount(y_data)
    
    # Check for empty classes or single-class data
    if len(class_counts) < 2 or 0 in class_counts:
        if verbose:
            print("  Warning: One class is missing or data is not diverse, skipping WeightedSampler.")
        return None
        
    class_weights = 1.0 / class_counts
    sample_weights = np.array([class_weights[t] for t in y_data])
    sample_weights = torch.from_numpy(sample_weights).float()
    
    return WeightedRandomSampler(sample_weights, len(sample_weights))

# =============================================================================
# 2. TRAINING & EVALUATION CORE
# =============================================================================

def train_shapelet_model(model, train_loader, device, epochs=50, lr=0.001, verbose=0, val_loader=None, patience=10):
    """
    Train a single SoftShapeNet model with optional early stopping.
    
    Args:
        model: PyTorch model instance.
        train_loader: DataLoader for training.
        device: Device to train on.
        epochs (int): Max epochs.
        lr (float): Learning rate.
        verbose (int): Verbosity level.
        val_loader: DataLoader for validaton (optional).
        patience (int): Early stopping patience.
        
    Returns:
        tuple: (trained_model, best_epoch)
    """
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)
    
    best_val_loss = float('inf')
    best_model_state = None
    best_epoch = -1
    patience_counter = 0
    
    for epoch in range(epochs):
        model.train()
        epoch_loss = 0.0
        
        for xb, yb in train_loader:
            xb, yb = xb.to(device), yb.to(device)
            optimizer.zero_grad()
            
            # Mixture of Experts loss used for regularization
            logits, moe_loss = model(xb, num_epoch_i=epoch, warm_up_epoch=max(1, epochs//3))
            loss = criterion(logits, yb) + 0.001 * moe_loss
            
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            epoch_loss += loss.item()
        
        avg_train_loss = epoch_loss / len(train_loader)

        if val_loader is not None:
            # Validation phase for early stopping
            model.eval()
            val_loss = 0.0
            with torch.no_grad():
                for xb, yb in val_loader:
                    xb, yb = xb.to(device), yb.to(device)
                    logits, moe_loss = model(xb, num_epoch_i=epoch, warm_up_epoch=max(1, epochs//3))
                    loss = criterion(logits, yb) + 0.001 * moe_loss
                    val_loss += loss.item()
            
            avg_val_loss = val_loss / len(val_loader)
            if verbose > 0 and (epoch + 1) % 10 == 0:
                print(f"  Epoch {epoch+1}/{epochs}, Train Loss: {avg_train_loss:.4f}, Val Loss: {avg_val_loss:.4f}")

            if avg_val_loss < best_val_loss:
                best_val_loss = avg_val_loss
                best_model_state = copy.deepcopy(model.state_dict())
                best_epoch = epoch + 1
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= patience:
                    if verbose > 0:
                        print(f"  Early stopping at epoch {epoch+1}. Best epoch was {best_epoch}.")
                    break
        else:
            # No validation set
            if verbose > 0 and (epoch + 1) % 10 == 0:
                print(f"  Epoch {epoch+1}/{epochs}, Loss: {avg_train_loss:.4f}")
            best_epoch = epoch + 1
            best_model_state = model.state_dict()

    # Restore best model
    if best_model_state is not None:
        model.load_state_dict(best_model_state)
        
    return model, best_epoch

def evaluate_model(model, data_loader, device):
    """Evaluate model and return all predictions as a numpy array."""
    model.eval()
    all_preds = []
    with torch.no_grad():
        for xb, _ in data_loader:
            xb = xb.to(device)
            logits, _ = model(xb)
            preds = torch.argmax(logits, dim=1)
            all_preds.extend(preds.cpu().numpy())
    return np.array(all_preds)

# =============================================================================
# 3. HYPERPARAMETER OPTIMIZATION (GRID SEARCH)
# =============================================================================

def _train_and_evaluate_fold(config, train_ds, val_ds, val_labels, seq_len, num_channels, device, 
                            epochs, batch_size, lr, patience, scoring_func, use_weighted_sampler, verbose):
    """Helper to train and evaluate a single CV fold."""
    
    # Setup DataLoaders
    sampler = None
    shuffle = True
    
    if use_weighted_sampler:
        # Extract labels from the subset
        y_train_fold = np.array([train_ds.dataset.y[i] for i in train_ds.indices])
        sampler = _create_sampler(y_train_fold, verbose=(verbose > 0))
        if sampler:
            shuffle = False
            
    train_loader = DataLoader(train_ds, batch_size=min(batch_size, len(train_ds)), shuffle=shuffle, sampler=sampler)
    val_loader = DataLoader(val_ds, batch_size=batch_size)
    
    # Initialize Model
    model = SoftShapeNet(
        seq_len=seq_len,
        shape_size=config.get('shape_size', 50),
        num_channels=num_channels,
        emb_dim=config.get('emb_dim', 128),
        num_classes=2,
        sparse_rate=config.get('sparse_rate', 0.5),
        depth=config.get('depth', 2),
        stride=config.get('stride', 4),
        num_experts=config.get('num_experts', 8)
    ).to(device)
    
    # Train
    model, best_epoch = train_shapelet_model(
        model, train_loader, device, epochs, lr, 
        verbose=0, val_loader=val_loader, patience=patience
    )
    
    # Evaluate
    fold_preds = evaluate_model(model, val_loader, device)
    fold_score_dict = scoring_func(val_labels, fold_preds)
    
    return fold_score_dict, best_epoch

def optimize_shapelets(X_train, y_train, X_test, y_test, 
                       search_configs, 
                       scoring_func=None,
                       cv=3,
                       input_format='auto',
                       epochs=50, 
                       batch_size=32,
                       lr=0.001,
                       patience=20,
                       verbose=1,
                       device=None,
                       use_weighted_sampler=True):
    """
    Grid search over SoftShapeNet configurations with Cross-Validation.
    
    Args:
        X_train, y_train: Training data and labels.
        X_test, y_test: Test data and labels for final evaluation.
        search_configs: List of dicts containing hyperparameters.
        scoring_func: Function (y_true, y_pred) -> dict of scores. Defaults to F1 binary.
        cv: Int (n_splits) or scikit-learn CV object. Defaults to StratifiedKFold(n_splits=3).
        input_format: 'NLC' (Time steps last), 'NCL' (Channels last, PyTorch native), or 'auto'.
        epochs, batch_size, lr, patience: Training parameters.
        verbose: Verbosity level.
        device: 'mps', 'cuda', 'cpu' or None for auto-detection.
        use_weighted_sampler: Whether to use weighted sampling for class imbalance.
        
    Returns:
        dict: A dictionary containing:
            - 'model': The best trained SoftShapeNet model.
            - 'test_score': The score on the test set.
            - 'test_preds': Final predictions on the test set.
            - 'best_config': Dictionary of the best hyperparameters found.
            - 'cv_score': The average cross-validation score for the best config.
    """
    # Defaults
    if scoring_func is None:
        def scoring_func(y_true, y_pred):
            # Return dict to match helper expectation
            acc = (y_true == y_pred).mean()
            f1 = f1_score(y_true, y_pred, average='binary', zero_division=0)
            return {'f1': f1, 'accuracy': acc, 'balanced_accuracy': acc} # Simplified for default
            
    if device is None:
        device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
    
    # Handle Input Format / Transposition --> (N, C, L) required for SoftShapeNet
    if input_format == 'auto':
        if X_train.shape[1] > X_train.shape[2]: # (N, L, C)
            X_train_pt = X_train.transpose(0, 2, 1)
            X_test_pt = X_test.transpose(0, 2, 1)
        else:
            X_train_pt = X_train
            X_test_pt = X_test
    elif input_format == 'NLC':
        X_train_pt = X_train.transpose(0, 2, 1)
        X_test_pt = X_test.transpose(0, 2, 1)
    else: # 'NCL'
        X_train_pt = X_train
        X_test_pt = X_test
    
    y_train_np = np.array(y_train)
    y_test_np = np.array(y_test)

    seq_len = X_train_pt.shape[2]
    num_channels = X_train_pt.shape[1]
    
    # Datasets
    full_train_ds = ShapeletDataset(X_train_pt, y_train_np)
    test_ds = ShapeletDataset(X_test_pt, y_test_np)
    test_loader = DataLoader(test_ds, batch_size=batch_size)

    # CV Strategy
    if isinstance(cv, int):
        cv_strategy = StratifiedKFold(n_splits=cv, shuffle=True, random_state=42)
    else:
        cv_strategy = cv

    if verbose > 0:
        print(f"--- SoftShapeNet Optimization over {len(search_configs)} configurations ---")
        print(f"CV: {cv_strategy}")
        print(f"Device: {device}, Seq Len: {seq_len}, Channels: {num_channels}")

    best_avg_score = -float('inf')
    best_config = None

    # --- GRID SEARCH LOOP ---
    for idx, config in enumerate(search_configs):
        if verbose > 0:
            print(f"\n[{idx+1}/{len(search_configs)}] Config: {config}")
        
        fold_scores = []
        fold_best_epochs = []
        
        for fold, (train_idx, val_idx) in enumerate(cv_strategy.split(X_train_pt, y_train_np)):
            fold_train_ds = Subset(full_train_ds, train_idx)
            fold_val_ds = Subset(full_train_ds, val_idx)
            fold_val_labels = y_train_np[val_idx]
            
            fold_metrics, best_epoch = _train_and_evaluate_fold(
                config, fold_train_ds, fold_val_ds, fold_val_labels,
                seq_len, num_channels, device, epochs, batch_size, lr, patience, 
                scoring_func, use_weighted_sampler, verbose
            )
            
            # Handle both dictionary (default) and scalar (custom sklearn) scores
            if isinstance(fold_metrics, dict):
                # Try to get f1, then accuracy, then use the first value found
                score = fold_metrics.get('f1', fold_metrics.get('accuracy', next(iter(fold_metrics.values()))))
            else:
                score = fold_metrics
                
            fold_scores.append(score)
            fold_best_epochs.append(best_epoch)
            
            if verbose > 0:
                print(f"  Fold {fold+1}: Score={score:.4f} (Ep: {best_epoch})")
        
        avg_score = np.mean(fold_scores)
        avg_best_epoch = int(np.mean(fold_best_epochs))
        
        if verbose > 0:
            print(f"  Avg F1 Score: {avg_score:.4f}, Avg Best Epoch: {avg_best_epoch}")
        
        if avg_score > best_avg_score:
            best_avg_score = avg_score
            best_config = config.copy()
            best_config['optimal_epochs'] = avg_best_epoch

    # --- FINAL RETRAINING ---
    final_epochs = best_config.get('optimal_epochs', epochs)
    if verbose > 0:
        print(f"\n--- Retraining best config on full training set for {final_epochs} epochs ---")
        print(f"Best Config: {best_config}, CV Avg Score: {best_avg_score:.4f}")
    
    # Final Sampler
    final_sampler = None
    final_shuffle = True
    if use_weighted_sampler:
        final_sampler = _create_sampler(y_train_np, verbose=False)
        if final_sampler:
            final_shuffle = False

    full_train_loader = DataLoader(
        full_train_ds, 
        batch_size=batch_size, 
        shuffle=final_shuffle, 
        sampler=final_sampler
    )
    
    best_model = SoftShapeNet(
        seq_len=seq_len,
        shape_size=best_config['shape_size'],
        num_channels=num_channels,
        emb_dim=best_config['emb_dim'],
        num_classes=2,
        sparse_rate=best_config.get('sparse_rate', 0.5),
        depth=best_config.get('depth', 2),
        stride=best_config.get('stride', 4),
        num_experts=best_config.get('num_experts', 8)
    ).to(device)
    
    best_model, _ = train_shapelet_model(best_model, full_train_loader, device, final_epochs, lr, verbose=1 if verbose > 0 else 0)
    
    test_preds = evaluate_model(best_model, test_loader, device)
    test_score = scoring_func(y_test_np, test_preds)
    
    score_display = test_score['f1'] if isinstance(test_score, dict) and 'f1' in test_score else test_score
    if verbose > 0:
        if isinstance(score_display, float):
             print(f"\n--- Final Test Set Score: {score_display:.4f} ---")
        else:
             print(f"\n--- Final Test Set Score: {score_display} ---")
    
    return {
        'model': best_model,
        'test_score': test_score,
        'test_preds': test_preds,
        'best_config': best_config,
        'cv_score': best_avg_score
    }

# =============================================================================
# 4. ANALYSIS & INTERPRETATION
# =============================================================================

def extract_softshape_filters(model):
    """
    Extracts the convolutional filters (shapelets) from a trained SoftShapeNet model.
    
    Returns:
        numpy.ndarray: Shapelets with shape (num_filters, shape_len, num_channels)
                       ready for visualization.
    """
    # SoftShapeNet uses ShapeEmbedLayer -> Conv1d
    # Weights are (out_channels, in_channels, kernel_size)
    # We transpose to (out_channels, kernel_size, in_channels) for plotting
    weights = model.shape_embed.proj.weight.detach().cpu().numpy()
    return weights.transpose(0, 2, 1)

def get_shapelet_activations(model, X, input_format='NLC'):
    """
    Extracts shapelet activations (output of the first conv layer) for each sample.
    
    Args:
        model: Trained SoftShapeNet model.
        X: Input data (N, L, C) or (N, C, L).
        input_format: 'NLC' or 'NCL'.
        
    Returns:
        np.ndarray: Activations with shape (N, emb_dim, num_patches).
    """
    model.eval()
    device = next(model.parameters()).device
    
    X_pt = X.transpose(0, 2, 1) if input_format == 'NLC' else X
    X_tensor = torch.tensor(X_pt, dtype=torch.float32).to(device)
    
    with torch.no_grad():
        # Extracts only the output of the embedding layer (shapelet conv)
        activations = model.shape_embed.proj(X_tensor)  # (N, emb_dim, num_patches)
    
    return activations.cpu().numpy()

def analyze_shapelet_importance(model, X, y, input_format='NLC', class_names=None):
    """
    Analyzes which shapelets activate the most for each class.
    
    Args:
        model: Trained SoftShapeNet model.
        X: Input data.
        y: Labels (0, 1, ...).
        input_format: 'NLC' or 'NCL'.
        class_names: Class names (optional).
        
    Returns:
        dict: For each class, the mean and std of shapelet activations.
    """
    activations = get_shapelet_activations(model, X, input_format)
    # Aggregate by sample: mean of activations over all temporal positions
    # activations shape: (N, emb_dim, num_patches) -> (N, emb_dim)
    sample_activations = activations.mean(axis=2)
    
    y = np.array(y)
    classes = np.unique(y)
    
    if class_names is None:
        class_names = [f"Class {c}" for c in classes]
    
    results = {}
    for c, name in zip(classes, class_names):
        mask = (y == c)
        class_acts = sample_activations[mask]
        results[name] = {
            'mean': class_acts.mean(axis=0),  # (emb_dim,)
            'std': class_acts.std(axis=0),
            'n_samples': mask.sum()
        }
    
    return results

# =============================================================================
# 5. VISUALIZATION
# =============================================================================

def plot_shapelet_importance(importance_results, top_k=10, figsize=(12, 5)):
    """
    Visualizes the most discriminative shapelets between classes.
    
    Args:
        importance_results: Output of analyze_shapelet_importance.
        top_k: Number of top shapelets to show.
        figsize: Figure size.
    """
    class_names = list(importance_results.keys())
    means = [importance_results[c]['mean'] for c in class_names]
    
    # Calculate absolute difference between class means
    diff = np.abs(means[0] - means[1])
    top_indices = np.argsort(diff)[-top_k:][::-1]
    
    fig, axes = plt.subplots(1, 2, figsize=figsize)
    
    # Plot 1: Mean activations comparison for top shapelets
    x = np.arange(top_k)
    width = 0.35
    axes[0].bar(x - width/2, means[0][top_indices], width, label=class_names[0], alpha=0.8)
    axes[0].bar(x + width/2, means[1][top_indices], width, label=class_names[1], alpha=0.8)
    axes[0].set_xticks(x)
    axes[0].set_xticklabels([f"S{i}" for i in top_indices], fontsize=8)
    axes[0].set_xlabel("Shapelet ID")
    axes[0].set_ylabel("Mean Activation")
    axes[0].set_title(f"Top {top_k} Discriminative Shapelets")
    axes[0].legend()
    
    # Plot 2: Difference between classes
    axes[1].barh(x, diff[top_indices], color='coral')
    axes[1].set_yticks(x)
    axes[1].set_yticklabels([f"S{i}" for i in top_indices], fontsize=8)
    axes[1].set_xlabel("Absolute Difference")
    axes[1].set_title("Shapelet Discriminativity")
    
    plt.tight_layout()
    plt.show()
    
    return top_indices


def plot_shapelet_profiles(model, X, y, features, classes, colors=('royalblue', 'coral')):
    """plotter for binary shapelet discrimination."""
    # 1. Calc Importance & Find Top Indices
    imp = analyze_shapelet_importance(model, X, y, input_format='NLC', class_names=classes)
    idxs = [np.argmax(imp[classes[0]]['mean'] - imp[classes[1]]['mean']),
            np.argmax(imp[classes[1]]['mean'] - imp[classes[0]]['mean'])]

    # 2. Extract Weights & Plot
    weights = extract_softshape_filters(model)
    fig, axes = plt.subplots(len(features), 2, figsize=(10, 1.8 * len(features)), sharex=True)
    fig.suptitle(f"Discriminative Patterns: {classes[0]} vs {classes[1]}", y=1.02, fontsize=14)

    for i, (row, feat) in enumerate(zip(axes, features)):
        for j, ax in enumerate(row):
            ax.plot(weights[idxs[j], :, i], c=colors[j], lw=2.5, alpha=0.9)
            ax.grid(alpha=0.3)
            if j == 0: ax.set_ylabel(feat, weight='bold')
            if i == 0: ax.set_title(f"{classes[j]} (S{idxs[j]})", color=colors[j], weight='bold')

    plt.tight_layout()
    plt.show()

def process_audio_segments_pipeline(
    file_map: Dict[int, List[str]],
    feature_extractor: Callable,
    sr: int = 24000,
    chunk_sec: int = 30,
    test_size: float = 0.2,
    n_folds: int = 3,
    random_state: int = 42,
    **extractor_kwargs
) -> dict:
    """
    Executes the full audio pipeline: loading, chunking, feature extraction, 
    scaling, and group-aware splitting (preventing song leakage).

    Args:
        file_map: Dictionary {label: [file_paths]}.
        feature_extractor: Function to extract features from raw audio chunks.
        sr: Sampling rate.
        chunk_sec: Duration of each audio chunk in seconds.
        test_size: Proportion of songs (groups) to include in the test split.
        n_folds: Number of CV folds.
        **extractor_kwargs: Additional arguments passed to feature_extractor.

    Returns:
        dict: Contains 'X_train', 'y_train', 'X_test', 'y_test', and 'cv' (PredefinedSplit).
    """
    
    # 1. Load & Segment Audio
    chunk_samples = int(chunk_sec * sr)
    raw_chunks, labels, groups = [], [], []
    song_id_counter = 0

    print(f"Processing audio (SR={sr}, Chunk={chunk_sec}s)...")

    for label, files in file_map.items():
        for file_path in files:
            y, _ = librosa.load(file_path, sr=sr)
            
            # Handle duration: Pad short files, Frame long files
            if len(y) < chunk_samples:
                frames = [librosa.util.fix_length(y, size=chunk_samples)]
            else:
                frames = librosa.util.frame(y, frame_length=chunk_samples, hop_length=chunk_samples, axis=0)
            
            raw_chunks.extend(frames)
            labels.extend([label] * len(frames))
            groups.extend([song_id_counter] * len(frames))
            song_id_counter += 1

    # Convert to arrays
    raw_chunks = np.array(raw_chunks)
    labels = np.array(labels)
    groups = np.array(groups)

    # 2. Feature Extraction & Scaling
    print(f"Extracting features from {len(raw_chunks)} segments...")
    X_feat, _ = feature_extractor(raw_chunks, sr=sr, **extractor_kwargs)
    
    X_scaled = TimeSeriesScalerMeanVariance().fit_transform(to_time_series_dataset(X_feat))

    # 3. Group-Aware Train/Test Split
    # We split based on unique SONGS, not segments, to avoid leakage.
    unique_songs = np.unique(groups)
    song_labels = np.array([labels[groups == s][0] for s in unique_songs])

    train_songs, test_songs = train_test_split(
        unique_songs, test_size=test_size, stratify=song_labels, random_state=random_state
    )

    # Create masks
    train_mask = np.isin(groups, train_songs)
    test_mask = np.isin(groups, test_songs)

    X_train, y_train = X_scaled[train_mask], labels[train_mask]
    X_test, y_test = X_scaled[test_mask], labels[test_mask]
    groups_train = groups[train_mask]

    # 4. Prepare PredefinedSplit for Cross-Validation
    # We map training songs to specific folds ensuring all chunks of a song stay together.
    u_train_songs = np.unique(groups_train)
    u_train_labels = np.array([y_train[groups_train == s][0] for s in u_train_songs])
    
    skf = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=random_state)
    song_to_fold = {}
    
    for fold_idx, (_, val_idx) in enumerate(skf.split(u_train_songs, u_train_labels)):
        for s_idx in val_idx:
            song_to_fold[u_train_songs[s_idx]] = fold_idx

    # Assign each training chunk to its song's fold
    test_fold = np.array([song_to_fold[g] for g in groups_train])
    custom_cv = PredefinedSplit(test_fold)

    print(f"Pipeline Complete. Train: {len(X_train)}, Test: {len(X_test)}")
    
    return {
        "X_train": X_train, "y_train": y_train,
        "X_test": X_test, "y_test": y_test,
        "cv": custom_cv
    }