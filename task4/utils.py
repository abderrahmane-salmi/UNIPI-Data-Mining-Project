import re
import torch
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import librosa
from scipy.ndimage import gaussian_filter1d
from scipy.stats import skew, kurtosis
from sklearn.metrics import silhouette_samples
from IPython.display import display
from transformers import ClapModel, ClapProcessor
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import os
os.environ['PYTORCH_ENABLE_MPS_FALLBACK'] = '1'
# =============================================================================
# CONSTANTS & CONFIGURATION
# =============================================================================

# =============================================================================
# SEMANTIC LABELS - OPTIMIZED FOR DISCRIMINATION
# =============================================================================
# These labels are designed to be:
# 1. Contrasting (opposite concepts)
# 2. Concrete (audio characteristics, not abstract concepts)
# 3. Few per category (3-4 max to avoid overlap)

# --- PRIMARY LABELS (Recommended) ---

RAP_STYLES = [
    "Smooth melodic singing with autotune",
    "Energetic rhythmic flow with ad-libs",
    "Aggressive fast rapping with shouting"
]

PRODUCTION_LABELS = [
    "Acoustic instruments and live drums",
    "Heavy bass and 808 drums trap beat"
]

ENERGY_LABELS = [
    "High energy party anthem",
    "Chill relaxed vibe"
]

VOCAL_STYLE_LABELS = [
    "Heavy autotune pitch correction",
    "Single dry vocal track",
    "Raw unprocessed natural voice"
]

TEMPO_LABELS = [
    "Medium tempo around 100 BPM",
    "Fast tempo above 140 BPM",
    "Slow tempo below 80 BPM",
]

# --- SECONDARY LABELS (More specific) ---

# Structure category removed as per analysis recommendations
# STRUCTURE_LABELS = [...]

DENSITY_LABELS = [
    "Dense layered production with many elements",
    "Sparse minimalist beat with few elements"
]

VOICE_BALANCE_LABELS = [
    # Binary contrast  
    "Vocals louder than instrumental",
    "Instrumental louder than vocals",
]

# =============================================================================
# OLD LABELS (Preserved for reference - these had low discriminative power)
# =============================================================================

RAP_STYLES_OLD = [
    "Old School Boom Bap",       
    "Hardcore Aggressive Rap",   
    "Conscious Storytelling",    
    "Double Time Rap",          
    "Italian Pop Rap",         
    "Melodic Autotuned Rap",     
    "Electronic Club Rap",       
    "Modern Trap",              
    "Dark Emo Rap",             
    "Chill Lo-Fi Hip Hop",      
]

STRUCTURE_LABELS_OLD = [
    "Very repetitive sections",
    "Clear verse-chorus structure",
    "Evolving sections with noticeable changes",
    "Chaotic or fragmented structure",
    "Gradual build-up increasing intensity",
    "Minimal variation, repetitive loops",
    "Choruses differ significantly from verses"
]

DENSITY_LABELS_OLD = [
    "Very dense, many overlapping layers",
    "Moderately dense, balanced elements",
    "Sparse, few instruments, lots of space",
    "Alternating sparse and dense sections",
    "Cluttered with overlapping melodic/percussive elements",
    "Very light instrumentation, focus on silence/space"
]

VOICE_BALANCE_LABELS_OLD = [
    "Vocals clearly dominant",
    "Instrumental dominant with vocals blended",
    "Vocals and instrumental equally balanced",
    "Vocals dominate only in choruses",
    "Vocals blend in the background",
    "Vocals prominent in some passages, minimal in others"
]

TEMPO_PERCEPTION_LABELS_OLD = [
    "Fast and urgent",
    "Steady, moderate pacing",
    "Slow or stretched",
    "Variable tempo within sections",
    "Mostly slow with occasional bursts",
    "Irregular or syncopated, hard to perceive steady beat"
]

# Shortened Labels for Display
DISPLAY_SHORT_LABELS = {
    "Smooth melodic singing with autotune": "Melodic Autotune",
    "Energetic rhythmic flow with ad-libs": "Energetic Flow",
    "Aggressive fast rapping with shouting": "Aggressive Rap",
    
    "Acoustic instruments and live drums": "Acoustic/Live",
    "Heavy bass and 808 drums trap beat": "Trap/808",
    
    "High energy party anthem": "Party Anthem",
    "Chill relaxed vibe": "Chill Vibe",
    
    "Heavy autotune pitch correction": "Heavy Autotune",
    "Single dry vocal track": "Dry Vocal",
    "Raw unprocessed natural voice": "Raw Voice",
    
    "Medium tempo around 100 BPM": "Medium (100)",
    "Fast tempo above 140 BPM": "Fast (>140)",
    "Slow tempo below 80 BPM": "Slow (<80)",
    
    "Dense layered production with many elements": "Dense/Layered",
    "Sparse minimalist beat with few elements": "Sparse/Minimal"
}

# Global Map for Label Lookup
CATEGORY_TO_LABELS = {
    "Style": RAP_STYLES,
    "Production": PRODUCTION_LABELS,
    "Energy": ENERGY_LABELS,
    "Vocal": VOCAL_STYLE_LABELS,
    "Tempo": TEMPO_LABELS,
    "Density": DENSITY_LABELS,
    "Voice Balance": VOICE_BALANCE_LABELS
}

# Alias for backward compatibility
TEMPO_PERCEPTION_LABELS = TEMPO_LABELS


# Initialize CLAP Model globally
try:
    print("Loading CLAP Model...")
    model = ClapModel.from_pretrained("laion/clap-htsat-unfused", use_safetensors=True)
    processor = ClapProcessor.from_pretrained("laion/clap-htsat-unfused")
except Exception as e:
    print(f"Warning: Failed to load CLAP model: {e}")
    model = None
    processor = None

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def extract_track_id(filepath):
    """Extracts track ID (e.g., TR123) from a filepath string."""
    match = re.search(r"TR\d+", filepath)
    if match:
        return match.group()
    return None

# =============================================================================
# AUDIO PROCESSING FUNCTIONS
# =============================================================================


    

# =============================================================================
# CLAP CLASSIFICATION FUNCTIONS
# =============================================================================

def clap_classify(labels, y, sr, model_instance=None, processor_instance=None, temperature=1.0):
    """
    Classifies an audio segment using Zero-Shot CLAP against a set of text labels.
    
    Args:
        labels (list): List of text descriptions.
        y (np.ndarray): Audio signal.
        sr (int): Sampling rate.
        sr (int): Sampling rate.
        model_instance: Pretrained ClapModel (uses global 'model' if None).
        processor_instance: Pretrained ClapProcessor (uses global 'processor' if None).
        temperature (float): Temperature for softmax scaling (default 1.0). Lower values (<1.0) sharpen the distribution.
    """
    # Fallback to global variables if not provided
    local_model = model_instance if model_instance is not None else model
    local_processor = processor_instance if processor_instance is not None else processor
    
    if local_model is None or local_processor is None:
        raise RuntimeError("CLAP model/processor not initialized. Pass them explicitly or ensure global load succeeded.")

    inputs = local_processor(
        text=labels,
        audios=y,
        return_tensors="pt",
        padding=True,
        sampling_rate=sr
    )
    
    # Move inputs to device (MPS if available)
    device = next(local_model.parameters()).device
    inputs = {k: v.to(device) if hasattr(v, 'to') else v for k, v in inputs.items()}
    
    with torch.no_grad():
        outputs = local_model(**inputs)
    
    probs = (outputs.logits_per_audio / temperature).softmax(dim=1)
    return probs[0].cpu()

def clap_classify_full_song(labels, y, sr, model_instance=None, processor_instance=None, 
                             temperature=1.0, window_sec=10, hop_sec=5, aggregation='mean', batch_size=6):
    """
    Classifies an ENTIRE audio signal using windowed CLAP analysis with BATCH PROCESSING.
    Optimized for speed by encoding labels once and processing audio chunks in batches.
    
    Args:
        labels (list): List of text descriptions.
        y (np.ndarray): Full audio signal.
        sr (int): Sampling rate.
        model_instance: Pretrained ClapModel.
        processor_instance: Pretrained ClapProcessor.
        temperature (float): Temperature for softmax.
        window_sec (float): Window duration.
        hop_sec (float): Hop between windows.
        aggregation (str): 'mean', 'max', 'median'.
        batch_size (int): Number of windows to process at once.
    """
    local_model = model_instance if model_instance is not None else model
    local_processor = processor_instance if processor_instance is not None else processor
    
    if local_model is None or local_processor is None:
        raise RuntimeError("CLAP model/processor not initialized.")

    window_samples = int(window_sec * sr)
    hop_samples = int(hop_sec * sr)
    
    segments = []
    
    # 1. Slice audio into segments
    if len(y) <= window_samples:
        segments.append(y)
    else:
        for start in range(0, len(y) - window_samples + 1, hop_samples):
            segments.append(y[start : start + window_samples])
            
        last_start = len(y) - window_samples
        # Avoid duplicate if the loop touched the end closely
        if len(segments) == 0 or (len(y) > window_samples and last_start > (len(segments)-1) * hop_samples):
             # Just take the last window valid
             segments.append(y[-window_samples:])
    
    if not segments:
        return torch.zeros(len(labels))

    # 2. Process in batches
    all_probs = []
    device = next(local_model.parameters()).device
    
    for i in range(0, len(segments), batch_size):
        batch_audio = segments[i : i + batch_size]
        
        # Processor handles list of numpy arrays
        inputs = local_processor(
            text=labels,
            audios=batch_audio,
            return_tensors="pt",
            padding=True,
            sampling_rate=sr
        )
        
        # Move to device
        inputs = {k: v.to(device) if hasattr(v, 'to') else v for k, v in inputs.items()}
        
        with torch.no_grad():
            outputs = local_model(**inputs)
            
        # Outputs: logits_per_audio is (Batch, N_Labels)
        logits = outputs.logits_per_audio
        probs = (logits / temperature).softmax(dim=1)
        
        all_probs.append(probs.cpu())
        
        # Explicit cleanup for memory
        del inputs, outputs, logits, probs
        if device.type == 'mps':
             torch.mps.empty_cache()

    # 3. Aggregate
    if all_probs:
        stacked = torch.cat(all_probs, dim=0).numpy() # (Total_Windows, N_Labels)
        
        if aggregation == 'mean':
            aggregated = np.mean(stacked, axis=0)
        elif aggregation == 'max':
            aggregated = np.max(stacked, axis=0)
        elif aggregation == 'median':
            aggregated = np.median(stacked, axis=0)
        else:
            raise ValueError(f"Unknown aggregation method: {aggregation}")
            
        return torch.tensor(aggregated)
    else:
        return torch.zeros(len(labels))

def evaluate_song_level(model, X_test, y_test, groups, input_format='NLC', class_names=None):
    """
    Evaluates a model at the song level by aggregating segment predictions.
    
    Args:
        model: Trained PyTorch model.
        X_test: Test array (N, L, C) or (N, C, L).
        y_test: Segment labels.
        groups: Array with song ID for each segment.
        input_format: 'NLC' or 'NCL'.
        class_names: List of class names for the report (optional).
        
    Returns:
        dict: {'f1': float, 'accuracy': float, 'song_preds': array, 'song_labels': array}
    """
    import torch.nn.functional as F
    from sklearn.metrics import f1_score, accuracy_score
    
    model.eval()
    device = next(model.parameters()).device
    
    # Transposition if necessary
    X_pt = X_test.transpose(0, 2, 1) if input_format == 'NLC' else X_test
    X_tensor = torch.tensor(X_pt, dtype=torch.float32).to(device)
    
    with torch.no_grad():
        logits, _ = model(X_tensor)
        probs = F.softmax(logits, dim=1).cpu().numpy()
    
    # Aggregate by song
    unique_songs = np.unique(groups)
    song_probs = np.array([probs[groups == s].mean(axis=0) for s in unique_songs])
    song_labels = np.array([y_test[groups == s][0] for s in unique_songs])
    song_preds = np.argmax(song_probs, axis=1)
    
    return {
        'f1': f1_score(song_labels, song_preds, average='binary', zero_division=0),
        'accuracy': accuracy_score(song_labels, song_preds),
        'song_preds': song_preds,
        'song_labels': song_labels,
        'song_probs': song_probs
    }

def evaluate_shapelet_classifier(y_true, y_pred, y_prob=None, class_names=None):
    """
    Evaluates a classifier with all metrics and plots (Confusion Matrix + ROC).
    
    Args:
        y_true: True labels.
        y_pred: Model predictions.
        y_prob: Probabilities for the positive class (optional, for ROC).
        class_names: Class names.
        
    Returns:
        dict: Dictionary containing all metrics.
    """
    from sklearn.metrics import (
        accuracy_score, precision_score, recall_score, f1_score, balanced_accuracy_score,
        classification_report, confusion_matrix, ConfusionMatrixDisplay,
        roc_curve, auc, RocCurveDisplay
    )
    
    if class_names is None:
        class_names = ['Class 0', 'Class 1']
    
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    
    # Metrics
    metrics = {
        'accuracy': accuracy_score(y_true, y_pred),
        'precision': precision_score(y_true, y_pred, average='binary', zero_division=0),
        'recall': recall_score(y_true, y_pred, average='binary', zero_division=0),
        'f1': f1_score(y_true, y_pred, average='binary', zero_division=0),
        'balanced_accuracy': balanced_accuracy_score(y_true, y_pred),
    }
    
    # Print metrics
    print("=" * 50)
    print("CLASSIFICATION METRICS")
    print("=" * 50)
    for k, v in metrics.items():
        print(f"{k.upper():12s}: {v:.4f}")
    
    # ROC/AUC only if we have probabilities
    if y_prob is not None:
        fpr, tpr, _ = roc_curve(y_true, y_prob)
        metrics['roc_auc'] = auc(fpr, tpr)
        print(f"{'ROC_AUC':12s}: {metrics['roc_auc']:.4f}")
    
    print("\n" + classification_report(y_true, y_pred, target_names=class_names))
    
    # Plots
    n_plots = 2 if y_prob is not None else 1
    fig, axes = plt.subplots(1, n_plots, figsize=(6 * n_plots, 5))
    if n_plots == 1:
        axes = [axes]
    
    cm = confusion_matrix(y_true, y_pred)
    ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=class_names).plot(ax=axes[0], cmap='Blues', values_format='d')
    axes[0].set_title("Confusion Matrix", fontsize=12, fontweight='bold')
    
    if y_prob is not None:
        RocCurveDisplay(fpr=fpr, tpr=tpr, roc_auc=metrics['roc_auc']).plot(ax=axes[1])
        axes[1].set_title(f"ROC Curve (AUC = {metrics['roc_auc']:.3f})", fontsize=12, fontweight='bold')
        axes[1].plot([0, 1], [0, 1], 'k--', alpha=0.5)
    
    plt.tight_layout()
    plt.show()
    
    return metrics



def apply_clap_labelling(cluster_samples, file_paths, sr=48000, model_instance=None, 
                          processor_instance=None, temperature=0.5, 
                          window_sec=10, hop_sec=5, aggregation='mean', cached_probs=None):
    """
    Applies CLAP classification to clusters of samples to derive semantic profiles.
    
    Args:
        cluster_samples (list of lists): Indices of songs in each cluster.
        file_paths (list): List of file paths corresponding to the indices in cluster_samples.
        sr (int): Sampling rate.
        model_instance: CLAP model instance (ignored if cached_probs provided).
        processor_instance: CLAP processor instance (ignored if cached_probs provided).
        temperature, window_sec, hop_sec: Analysis parameters (ignored if cached_probs).
        aggregation: Aggregation method.
        cached_probs (dict): Pre-computed sample_probs from load_clap_labels(). If provided, avoids re-computation.
    
    Returns:
        tuple: (cluster_prompt_means, sample_probs)
    """
    from tqdm import tqdm
    
    cluster_prompt_means = []
    
    # If using cache, use it directly
    if cached_probs is not None:
        print("Using cached CLAP probabilities...")
        sample_probs = cached_probs
    else:
        sample_probs = {}
    
    # Identify missing songs
    all_songs = [(cluster_idx, song_idx) for cluster_idx, cluster in enumerate(cluster_samples) for song_idx in cluster]
    songs_to_process = [s for s in all_songs if s[1] not in sample_probs]
    
    if songs_to_process:
        print(f"Classifying {len(songs_to_process)} songs with full-song CLAP analysis (window={window_sec}s)...")
        
        for cluster_idx, song_idx in tqdm(songs_to_process, desc="CLAP Full-Song Labelling"):
            if song_idx in sample_probs: continue
                
            path = file_paths[song_idx]
            y, _ = librosa.load(path, sr=sr)
            
            sample_probs[song_idx] = {}
            
            # Expanded category list
            categories = [
                ("Style", RAP_STYLES),
                ("Production", PRODUCTION_LABELS),
                ("Production", PRODUCTION_LABELS),
                # ("Energy", ENERGY_LABELS),
                ("Vocal", VOCAL_STYLE_LABELS),
                ("Tempo", TEMPO_LABELS),
                ("Structure", STRUCTURE_LABELS),
                ("Density", DENSITY_LABELS)
            ]
            
            for name, labels in categories:
                probs = clap_classify_full_song(
                    labels, y, sr, 
                    model_instance=model_instance, 
                    processor_instance=processor_instance,
                    temperature=temperature,
                    window_sec=window_sec,
                    hop_sec=hop_sec,
                    aggregation=aggregation
                )
                
                p_np = probs.detach().cpu().numpy() if hasattr(probs, 'detach') else probs.numpy()
                sample_probs[song_idx][name] = p_np
    
    # Now compute cluster means for ALL available categories
    print("Computing cluster means...")
    all_categories = ["Style", "Production", "Vocal", "Tempo", "Structure", "Density"]
    
    for cluster_idx, cluster in enumerate(cluster_samples):
        cluster_dict = {}
        
        for name in all_categories:
            probs_list = []
            for song_idx in cluster:
                # Robustly check if song and category exist
                if song_idx in sample_probs and name in sample_probs[song_idx]:
                    probs_list.append(sample_probs[song_idx][name])
            
            if probs_list:
                mean_probs = np.mean(np.stack(probs_list), axis=0)  
            else:
                mean_probs = None
                
            cluster_dict[name] = mean_probs

        cluster_prompt_means.append(cluster_dict)

    return cluster_prompt_means, sample_probs



def precompute_clap_labels(file_paths, output_path="./clap_labels_cache.npz", 
                           sr=48000, model_instance=None, processor_instance=None,
                           temperature=0.5, window_sec=10, hop_sec=10, aggregation='mean', batch_size=16):
    """
    Pre-computes CLAP semantic labels for all files and saves to disk.
    
    Args:
        ...
        batch_size (int): Batch size for inference (default 16, good for M1/Pro CPUs).
    """
    from tqdm import tqdm
    import json
    
    local_model = model_instance if model_instance is not None else model
    local_processor = processor_instance if processor_instance is not None else processor
    
    if local_model is None or local_processor is None:
        raise RuntimeError("CLAP model/processor not initialized.")
    
    categories = [
        ("Style", RAP_STYLES),
        ("Production", PRODUCTION_LABELS),
        # ("Energy", ENERGY_LABELS),
        ("Vocal", VOCAL_STYLE_LABELS),
        ("Tempo", TEMPO_LABELS),
        # ("Structure", STRUCTURE_LABELS), # Removed
        ("Density", DENSITY_LABELS),
    ]
    
    sample_probs = {}
    
    print(f"Pre-computing CLAP labels for {len(file_paths)} files...")
    print(f"Settings: temp={temperature}, window={window_sec}s, hop={hop_sec}s, agg={aggregation}, batch={batch_size}")
    print(f"Output: {output_path}")
    
    for idx, path in enumerate(tqdm(file_paths, desc="CLAP Pre-computation")):
        try:
            y, _ = librosa.load(path, sr=sr)
            
            sample_probs[idx] = {"file_path": path}
            
            for name, labels in categories:
                probs = clap_classify_full_song(
                    labels, y, sr,
                    model_instance=local_model,
                    processor_instance=local_processor,
                    temperature=temperature,
                    window_sec=window_sec,
                    hop_sec=hop_sec,
                    aggregation=aggregation,
                    batch_size=batch_size
                )
                
                p_np = probs.detach().cpu().numpy() if hasattr(probs, 'detach') else probs.numpy()
                sample_probs[idx][name] = p_np
                
        except Exception as e:
            print(f"Error processing {path}: {e}")
            sample_probs[idx] = {"file_path": path, "error": str(e)}
    
    # Save to disk
    # Convert to format suitable for np.savez
    save_dict = {
        "file_paths": np.array(file_paths, dtype=object),
        "categories": np.array([name for name, _ in categories]),
        "labels": {name: np.array(labels, dtype=object) for name, labels in categories},
        "temperature": temperature,
        "window_sec": window_sec,
        "hop_sec": hop_sec,
        "aggregation": aggregation,
    }
    
    # Save probability arrays
    for idx in sample_probs:
        for name, _ in categories:
            if name in sample_probs[idx]:
                save_dict[f"probs_{idx}_{name}"] = sample_probs[idx][name]
    
    np.savez_compressed(output_path, **save_dict)
    print(f"\n✅ Cached {len(sample_probs)} samples to {output_path}")
    
    return sample_probs


def load_clap_labels(cache_path="./clap_labels_cache.npz"):
    """
    Loads pre-computed CLAP labels from cache.
    
    Args:
        cache_path (str): Path to the cached .npz file.
    
    Returns:
        dict: sample_probs dictionary {idx: {category: probs_array, "file_path": str}}
    
    Example:
        >>> sample_probs = load_clap_labels("./clap_cache.npz")
        >>> print(sample_probs[0]["Style"])  # Probability distribution for song 0
    """
    if not os.path.exists(cache_path):
        raise FileNotFoundError(f"Cache file not found: {cache_path}")
    
    data = np.load(cache_path, allow_pickle=True)
    
    file_paths = data["file_paths"]
    categories = data["categories"]
    
    sample_probs = {}
    
    for idx in range(len(file_paths)):
        sample_probs[idx] = {"file_path": str(file_paths[idx])}
        
        for name in categories:
            key = f"probs_{idx}_{name}"
            if key in data:
                sample_probs[idx][name] = data[key]
    
    print(f"✅ Loaded {len(sample_probs)} samples from {cache_path}")
    
    # Print cache metadata
    if "temperature" in data:
        print(f"   Settings: temp={data['temperature']}, window={data['window_sec']}s, hop={data['hop_sec']}s")
    
    return sample_probs


def aggregate_by_cluster(sample_probs, cluster_samples, categories=None):
    """
    Aggregates pre-computed sample labels by cluster to get cluster means.
    
    Args:
        sample_probs (dict): Pre-computed {idx: {category: probs}} from load_clap_labels().
        cluster_samples (list of lists): Indices of samples per cluster.
        categories (list): List of category names. If None, uses all available.
    
    Returns:
        tuple: (cluster_prompt_means, sample_probs)
        - cluster_prompt_means (list): Mean probability per cluster per category.
        - sample_probs (dict): The input sample_probs (passed through for compatibility).
    
    Example:
        >>> sample_probs = load_clap_labels("./clap_cache.npz")
        >>> cluster_samples = [[0, 1, 2], [3, 4, 5]]  # 2 clusters
        >>> cluster_means, _ = aggregate_by_cluster(sample_probs, cluster_samples)
    """
    if categories is None:
        # Detect categories from first sample
        first_idx = list(sample_probs.keys())[0]
        categories = [k for k in sample_probs[first_idx].keys() if k != "file_path" and k != "error"]
    
    cluster_prompt_means = []
    
    for cluster_idx, cluster in enumerate(cluster_samples):
        cluster_dict = {}
        
        for name in categories:
            probs_list = []
            
            for song_idx in cluster:
                if song_idx in sample_probs and name in sample_probs[song_idx]:
                    probs_list.append(sample_probs[song_idx][name])
            
            if probs_list:
                mean_probs = np.mean(np.stack(probs_list), axis=0)
            else:
                mean_probs = None
                
            cluster_dict[name] = mean_probs
        
        cluster_prompt_means.append(cluster_dict)
    
    return cluster_prompt_means, sample_probs

# =============================================================================
# EVALUATION FUNCTIONS
# =============================================================================
from sklearn.metrics import silhouette_samples
from scipy.spatial.distance import pdist, squareform, jensenshannon
from IPython.display import display

def evaluate_clustering_performance(X, labels, model=None, 
                                    sample_probs=None, method_name="Clustering",
                                    cached_probs=None, file_paths=None):
    """
    Evaluates clustering performance.
    
    Args:
        X (np.ndarray): Feature matrix.
        labels (np.ndarray): Cluster labels.
        sample_probs (dict, optional): Pre-aligned {index: probs} dict.
        cached_probs (dict, optional): Raw cache from load_clap_labels.
        file_paths (list, optional): List of file paths matching X. 
                                     Used to align cached_probs if sample_probs is None.
    """
    
    # --- 1. DATA ALIGNMENT (Cache -> Sample Probs) ---
    # This must happen BEFORE delegation because the TS function doesn't handle raw cache alignment.
    if sample_probs is None and cached_probs is not None and file_paths is not None:
        print(f"Aligning semantic cache for {len(file_paths)} files...")
        sample_probs = {}
        basename_map = {}
        for k, v in cached_probs.items():
            data = v.item() if hasattr(v, 'item') and isinstance(v.item(), dict) else v
            if isinstance(data, dict) and "file_path" in data:
                basename_map[os.path.basename(data["file_path"])] = data
        
        match_count = 0
        for idx, fpath in enumerate(file_paths):
            bn = os.path.basename(fpath)
            if bn in basename_map:
                sample_probs[idx] = basename_map[bn]
                match_count += 1
        print(f"Aligned {match_count}/{len(file_paths)} files with semantic data.")

    # --- 2. DELEGATION TO TIME SERIES EVALUATOR ---
    # If Input is 3D (Time Series), hand off to the specialized function.
    if X.ndim == 3:
        # Check if we should delegate (only if labels match samples, i.e. song-level clustering)
        if len(labels) == X.shape[0]:
            print("Detected 3D Time Series data. Delegating to evaluate_timeseries_clustering_performance...")
            return evaluate_timeseries_clustering_performance(
                X=X, 
                labels=labels, 
                model=model, 
                sample_probs=sample_probs, 
                method_name=method_name
            )
        else:
             print(f"Warning: 3D Input but label mismatch (Labels: {len(labels)} vs Samples: {X.shape[0]}). Flattening...")
             X = X.reshape(-1, X.shape[-1])

    unique_labels = np.unique(labels)
    valid_labels = [l for l in unique_labels if l != -1]
    total_samples = len(X)
    
    # =========================================================================
    # 1. SILHOUETTE CALCULATION (SEMANTIC & EUCLIDEAN)
    # =========================================================================
    
    global_sil_euclid = float('nan')
    global_sil_semantic = float('nan')
    sample_sil_euclid = np.zeros(total_samples)
    sample_sil_semantic = np.zeros(total_samples)

    try:
        if len(unique_labels) > 1:
            # --- STANDARD EUCLIDEAN SILHOUETTE ---
            sample_sil_euclid = silhouette_samples(X, labels)
            global_sil_euclid = np.mean(sample_sil_euclid)
            
            if sample_probs:
                # --- SEMANTIC SILHOUETTE CALCULATION (Jensen-Shannon) ---
                print("Calculating Semantic Silhouette Matrix (Jensen-Shannon)...")
                
                # 1. Identify common categories
                first_key = list(sample_probs.keys())[0]
                categories = list(sample_probs[first_key].keys())
                
                # 2. Build Distance Matrices for each category
                n_samples = len(X)
                combined_dist_matrix = np.zeros((n_samples, n_samples))
                
                valid_cats_count = 0
                
                for cat in categories:
                    # Extract probabilities for this category for ALL samples in order 0..N
                    probs_matrix = []
                    for i in range(n_samples):
                        if i in sample_probs and cat in sample_probs[i]:
                            probs_matrix.append(sample_probs[i][cat])
                        else:
                            # Fallback if missing: uniform distribution (max entropy) or skip
                            # For safety, let's assume valid data or append zeros
                            probs_matrix.append(np.zeros_like(sample_probs[first_key][cat]))

                    probs_matrix = np.array(probs_matrix)
                    
                    # Compute pairwise JS distances for this category
                    # pdist returns condensed matrix, squareform makes it NxN
                    # metric='jensenshannon' is available in recent scipy versions
                    try:
                        dists = pdist(probs_matrix, metric='jensenshannon')
                        # Check for NaNs (can happen if probs sum to 0)
                        dists = np.nan_to_num(dists, nan=1.0) 
                        combined_dist_matrix += squareform(dists)
                        valid_cats_count += 1
                    except Exception as e:
                        print(f"Warning: Failed to compute distances for category {cat}: {e}")

                # 3. Average the distances & Compute Semantic Silhouette
                if valid_cats_count > 0:
                    combined_dist_matrix /= valid_cats_count
                    sample_sil_semantic = silhouette_samples(combined_dist_matrix, labels, metric='precomputed')
                    global_sil_semantic = np.mean(sample_sil_semantic)
                else:
                    print("Warning: No valid semantic categories found for distance.")

        else:
            global_sil_euclid = -1 
            global_sil_semantic = -1
            
    except Exception as e:
        print(f"Error calculating silhouette: {e}")

    # Inertia (remains structural, related to k-means model)
    inertia = model.inertia_ if model and hasattr(model, 'inertia_') else float('nan')

    # =========================================================================
    # 2. INTRA-CLUSTER COHERENCE (REMOVED)
    # =========================================================================
    # User requested removal of manual intra-cluster coherence.
    global_intra = float('nan')

    # =========================================================================
    # 3. CLUSTER STATISTICS
    # =========================================================================
    cluster_stats = []

    for label in unique_labels:
        mask = (labels == label)
        points_in_cluster = X[mask]
        size = len(points_in_cluster)
        
    # =========================================================================
    # 3. CLUSTER STATISTICS
    # =========================================================================
    cluster_stats = []

    for label in unique_labels:
        mask = (labels == label)
        points_in_cluster = X[mask]
        size = len(points_in_cluster)
        
        # Compactness
        if size > 0:
            centroid = np.mean(points_in_cluster, axis=0)
            distances = np.linalg.norm(points_in_cluster - centroid, axis=1)
            mean_distance = np.mean(distances)
            
            # Local Silhouettes
            sil_euc = np.mean(sample_sil_euclid[mask]) if len(sample_sil_euclid) > 0 else 0
            sil_sem = np.mean(sample_sil_semantic[mask]) if len(sample_sil_semantic) > 0 else float('nan')
        else:
            mean_distance = 0
            sil_euc = 0
            sil_sem = 0
        
        cluster_stats.append({
            "Cluster ID": int(label),
            "Size": int(size),
            "Percentage": (size / total_samples) * 100,
            "Compactness (Eucl)": mean_distance,
            "Sil (Eucl)": sil_euc,
            "Sil (Sem)": sil_sem,
            "Sil (Sem)": sil_sem,
            # "Intra-Coh (Sem)": per_cluster_intra.get(int(label), float('nan'))
        })

    # =========================================================================
    # 4. OUTPUT AND REPORTING
    # =========================================================================
    df_analysis = pd.DataFrame(cluster_stats)
    
    balance_std = df_analysis["Percentage"].std() if len(df_analysis) > 1 else 0.0
    avg_compactness = df_analysis["Compactness (Eucl)"].mean()
    min_cluster_size = df_analysis["Size"].min()

    print(f"--- RESULTS: {method_name} ---")
    print(f"Global -> Inertia: {inertia:.1f}")
    print(f"Structural -> Silhouette (Eucl): {global_sil_euclid:.3f} | Avg Compactness: {avg_compactness:.3f}")
    print(f"Semantic   -> Silhouette (Sem): {global_sil_semantic:.3f}")
    print(f"Stats      -> Balance Std: {balance_std:.2f}% | Min Size: {min_cluster_size}")
    
    df_display = df_analysis.copy()
    df_display["Percentage"] = df_display["Percentage"].round(2).astype(str) + "%"
    df_display = df_display.round(4)
    df_display.set_index("Cluster ID", inplace=True)
    df_display = df_display.sort_values(by="Size", ascending=False)
    
    print("\n--- CLUSTER DETAILS ---")
    display(df_display)

    if sample_probs:
        print("\n--- MEAN SEMANTIC PROFILES ---")
        plot_semantic_bars(labels, sample_probs, file_paths=file_paths)

    global_metrics = {
        "Method": method_name,
        "Inertia": inertia,
        "Silhouette_Euclidean": global_sil_euclid,
        "Silhouette_Semantic": global_sil_semantic,
        # "Intra_Coherence": global_intra,
        "Avg_Compactness": avg_compactness,
        "Balance_Std": balance_std,
        "N_Clusters": len(unique_labels)
    }

    return global_metrics, df_analysis


def evaluate_timeseries_clustering_performance(X, labels, model=None, cluster_prompt_means=None, 
                                                sample_probs=None, method_name="TS Clustering",
                                                use_dtw_silhouette=False):
    """
    Evaluates clustering performance for TIME SERIES data (3D arrays).
    
    This function is specifically designed to handle 3D arrays with shape:
    (n_samples, n_timesteps, n_features)
    
    Args:
        X (np.ndarray): Time series matrix with shape (n_samples, n_timesteps, n_features).
        labels (np.ndarray): Cluster labels, one per sample (length = n_samples).
        model: Optional clustering model (for inertia extraction).
        cluster_prompt_means (list): Optional list of dicts with mean probabilities per cluster.
        sample_probs (dict): Optional {song_idx: {category: probs_array}} for Semantic Silhouette.
        method_name (str): Name of the clustering method for reporting.
        use_dtw_silhouette (bool): If True, compute silhouette using DTW distance (slower).
    
    Returns: 
        (global_metrics_dict, detailed_stats_dataframe)
    """
    
    # --- INPUT VALIDATION ---
    if X.ndim != 3:
        raise ValueError(f"Expected 3D array (n_samples, timesteps, features), got shape {X.shape}")
    
    n_samples, n_timesteps, n_features = X.shape
    
    if len(labels) != n_samples:
        raise ValueError(f"Mismatch: X has {n_samples} samples but labels has {len(labels)} elements")
    
    # --- FLATTEN PER SAMPLE for Euclidean distance ---
    # (n_samples, timesteps, features) -> (n_samples, timesteps * features)
    X_flat = X.reshape(n_samples, -1)
    
    unique_labels = np.unique(labels)
    valid_labels = [l for l in unique_labels if l != -1]
    
    # =========================================================================
    # 1. SILHOUETTE CALCULATION
    # =========================================================================
    
    global_sil_euclid = float('nan')
    global_sil_semantic = float('nan')
    global_sil_dtw = float('nan')
    sample_sil_euclid = np.zeros(n_samples)
    sample_sil_semantic = np.zeros(n_samples)

    try:
        if len(unique_labels) > 1:
            # --- STANDARD EUCLIDEAN SILHOUETTE (on flattened data) ---
            sample_sil_euclid = silhouette_samples(X_flat, labels)
            global_sil_euclid = np.mean(sample_sil_euclid)
            
            # --- DTW SILHOUETTE (optional, slow) ---
            if use_dtw_silhouette:
                try:
                    from tslearn.metrics import dtw
                    print("Computing DTW Silhouette (this may take a while)...")
                    
                    # Compute pairwise DTW distance matrix
                    dtw_dist_matrix = np.zeros((n_samples, n_samples))
                    for i in range(n_samples):
                        for j in range(i + 1, n_samples):
                            d = dtw(X[i], X[j])
                            dtw_dist_matrix[i, j] = d
                            dtw_dist_matrix[j, i] = d
                    
                    sample_sil_dtw = silhouette_samples(dtw_dist_matrix, labels, metric='precomputed')
                    global_sil_dtw = np.mean(sample_sil_dtw)
                except ImportError:
                    print("Warning: tslearn not installed, skipping DTW silhouette.")
                except Exception as e:
                    print(f"Warning: DTW silhouette failed: {e}")
            
            # --- SEMANTIC SILHOUETTE (if sample_probs provided) ---
            if sample_probs:
                print("Calculating Semantic Silhouette Matrix (Jensen-Shannon)...")
                
                first_key = list(sample_probs.keys())[0]
                categories = list(sample_probs[first_key].keys())
                
                combined_dist_matrix = np.zeros((n_samples, n_samples))
                valid_cats_count = 0
                
                for cat in categories:
                    probs_matrix = []
                    for i in range(n_samples):
                        if i in sample_probs and cat in sample_probs[i]:
                            probs_matrix.append(sample_probs[i][cat])
                        else:
                            probs_matrix.append(np.zeros_like(sample_probs[first_key][cat]))

                    probs_matrix = np.array(probs_matrix)
                    
                    try:
                        dists = pdist(probs_matrix, metric='jensenshannon')
                        dists = np.nan_to_num(dists, nan=1.0) 
                        combined_dist_matrix += squareform(dists)
                        valid_cats_count += 1
                    except Exception as e:
                        print(f"Warning: Failed to compute distances for category {cat}: {e}")

                if valid_cats_count > 0:
                    combined_dist_matrix /= valid_cats_count
                    sample_sil_semantic = silhouette_samples(combined_dist_matrix, labels, metric='precomputed')
                    global_sil_semantic = np.mean(sample_sil_semantic)

        else:
            global_sil_euclid = -1 
            global_sil_semantic = -1
            global_sil_dtw = -1
            
    except Exception as e:
        print(f"Error calculating silhouette: {e}")

    # Inertia (from k-means model if available)
    inertia = model.inertia_ if model and hasattr(model, 'inertia_') else float('nan')

    # =========================================================================
    # 2. SEMANTIC COHERENCE METRICS (INTRA-CLUSTER)
    # =========================================================================
    
    per_cluster_intra = {}
    global_intra = 0.0
    
    if sample_probs and cluster_prompt_means:
        intra_scores = []
        for lab in valid_labels:
            indices = np.where(labels == lab)[0]
            if len(indices) == 0: 
                continue
            
            cluster_idx = int(lab)
            if cluster_idx >= len(cluster_prompt_means): 
                continue
            
            means = cluster_prompt_means[cluster_idx]
            
            cat_scores = []
            for cat, mean_prob in means.items():
                if mean_prob is None: 
                    continue
                sample_ps = [sample_probs[i][cat] for i in indices if i in sample_probs and cat in sample_probs[i]]
                if not sample_ps: 
                    continue
                
                dists = [jensenshannon(p, mean_prob) for p in sample_ps]
                cat_scores.append(1.0 - np.mean(dists))
            
            c_intra = np.mean(cat_scores) if cat_scores else 0
            per_cluster_intra[lab] = c_intra
            intra_scores.append(c_intra)
        
        global_intra = np.mean(intra_scores) if intra_scores else 0

    # =========================================================================
    # 3. CLUSTER STATISTICS
    # =========================================================================
    cluster_stats = []

    for label in unique_labels:
        mask = (labels == label)
        points_in_cluster = X_flat[mask]  # Use flattened for compactness calc
        size = len(points_in_cluster)
        
        # Compactness (on flattened representation)
        if size > 0:
            centroid = np.mean(points_in_cluster, axis=0)
            distances = np.linalg.norm(points_in_cluster - centroid, axis=1)
            mean_distance = np.mean(distances)
            
            # Local Silhouettes
            sil_euc = np.mean(sample_sil_euclid[mask])
            sil_sem = np.mean(sample_sil_semantic[mask]) if sample_probs else float('nan')
        else:
            mean_distance = 0
            sil_euc = 0
            sil_sem = 0
        
        cluster_stats.append({
            "Cluster ID": int(label),
            "Size": int(size),
            "Percentage": (size / n_samples) * 100,
            "Compactness": mean_distance,
            "Sil (Eucl)": sil_euc,
            "Sil (Sem)": sil_sem,
            "Intra-Coh": per_cluster_intra.get(int(label), float('nan'))
        })

    # =========================================================================
    # 4. OUTPUT AND REPORTING
    # =========================================================================
    df_analysis = pd.DataFrame(cluster_stats)
    
    balance_std = df_analysis["Percentage"].std() if len(df_analysis) > 1 else 0.0
    avg_compactness = df_analysis["Compactness"].mean()
    min_cluster_size = df_analysis["Size"].min()

    print(f"--- RESULTS: {method_name} ---")
    print(f"Input Shape: {X.shape} (n_samples={n_samples}, timesteps={n_timesteps}, features={n_features})")
    print(f"Global -> Inertia: {inertia:.1f}" if not np.isnan(inertia) else "Global -> Inertia: N/A")
    print(f"Structural -> Silhouette (Eucl): {global_sil_euclid:.3f} | Avg Compactness: {avg_compactness:.3f}")
    if use_dtw_silhouette:
        print(f"DTW        -> Silhouette (DTW): {global_sil_dtw:.3f}")
    if sample_probs:
        print(f"Semantic   -> Silhouette (Sem): {global_sil_semantic:.3f} | Intra-Cluster Coherence: {global_intra:.3f}")
    print(f"Stats      -> Balance Std: {balance_std:.2f}% | Min Size: {min_cluster_size}")
    
    df_display = df_analysis.copy()
    df_display["Percentage"] = df_display["Percentage"].round(2).astype(str) + "%"
    df_display = df_display.round(4)
    df_display.set_index("Cluster ID", inplace=True)
    df_display = df_display.sort_values(by="Size", ascending=False)
    
    print("\n--- CLUSTER DETAILS ---")
    display(df_display)

    if sample_probs:
        print("\n--- MEAN SEMANTIC PROFILES ---")
        plot_semantic_bars(labels, sample_probs) # No file_paths in this scope, relies on index alignment

    global_metrics = {
        "Method": method_name,
        "Inertia": inertia,
        "Silhouette_Euclidean": global_sil_euclid,
        "Silhouette_DTW": global_sil_dtw if use_dtw_silhouette else float('nan'),
        "Silhouette_Semantic": global_sil_semantic,
        "Intra_Coherence": global_intra,
        "Avg_Compactness": avg_compactness,
        "Balance_Std": balance_std,
        "N_Clusters": len(unique_labels),
        "N_Samples": n_samples,
        "N_Timesteps": n_timesteps,
        "N_Features": n_features
    }

    return global_metrics, df_analysis


# =============================================================================
# DATA ANALYSIS & PLOTTING FUNCTIONS
# =============================================================================

def plot_ts(ts, sr=50, title="Time Series Plot", color='royalblue'):
    """Plots a single time series."""
    ts = np.array(ts).flatten()
    duration = len(ts) / sr
    time_axis = np.linspace(0, duration, len(ts))
    
    plt.figure(figsize=(12, 4))
    plt.plot(time_axis, ts, color=color, linewidth=1.5)
    plt.title(f"{title} (Total duration: {duration:.2f}s)", fontsize=14, fontweight='bold')
    plt.xlabel("Time (Seconds)") 
    plt.ylabel("Value")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()







def plot_cluster_composition(y_true, y_pred, title="Cluster Composition (Artist Purity)", figsize=(12, 6)):
    """
    Plots a stacked bar chart showing the percentage of each artist in each cluster.
    """
    # Create Crosstab
    df = pd.DataFrame({'True_Label': y_true, 'Cluster': y_pred})
    # Remove noise if present (-1)
    df = df[df['Cluster'] != -1]
    
    ct = pd.crosstab(df['Cluster'], df['True_Label'])
    
    # Normalize to get percentages
    ct_pct = ct.div(ct.sum(axis=1), axis=0) * 100
    
    # Plot
    ax = ct_pct.plot(kind='bar', stacked=True, figsize=figsize, colormap='coolwarm', alpha=0.9, edgecolor='black')
    
    plt.title(title, fontsize=16, fontweight='bold')
    plt.xlabel("Cluster ID", fontsize=12)
    plt.ylabel("Percentage (%)", fontsize=12)
    plt.legend(title='Artist', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    plt.xticks(rotation=0)
    
    # Annotate with total counts
    totals = ct.sum(axis=1)
    for i, total in enumerate(totals):
        ax.text(i, 102, f"n={total}", ha='center', fontsize=10, color='black', fontweight='bold')
        
    plt.tight_layout()
    plt.show()
    
    return ct






def visualize_cluster_profiles(centroids, cluster_prompt_means, tracks_df=None, tracks_ids=None, 
                               label_type="Style", prompt_list=None, sr=50, top_n=5, figsize=(18, 10)):
    """
    Visualizes cluster centroids alongside their semantic identity (CLAP scores).
    Supports multiple categories if provided in cluster_prompt_means.
    """
    # Define mapping between category name and its label list (must match apply_clap_labelling)
    category_map = {
        "Style": RAP_STYLES,
        "Production": PRODUCTION_LABELS,
        # "Energy": ENERGY_LABELS,
        "Vocal": VOCAL_STYLE_LABELS,
        "Tempo": TEMPO_LABELS,
        "Structure": STRUCTURE_LABELS,
        "Density": DENSITY_LABELS,
    }
    
    # If using old single-category mode, wrap it
    if label_type not in ["All", "Multi"]:
         # Fallback to single category logic (simulated by filtering later)
         categories_to_plot = [label_type]
    else:
         categories_to_plot = ["Style", "Production", "Vocal", "Tempo", "Density"] # Default set for "All"

    n_clusters = len(centroids)
    cmap = plt.get_cmap('tab10') 

    for i in range(n_clusters):
        ts = np.array(centroids[i]).flatten()
        
        # --- Metadata Logic (same as before) ---
        plot_title = "Song Info Not Available"
        if tracks_df is not None and tracks_ids is not None and i < len(tracks_ids):
            current_id = tracks_ids[i]
            row = tracks_df[tracks_df["id"] == current_id]
            if not row.empty:
                try:
                    song_name = row["title"].values[0]
                    author = row["name_artist"].values[0]
                    plot_title = f"{song_name} - {author} (ID: {current_id})"
                except KeyError: pass
            else:
                plot_title = f"ID {current_id} not found"

        # --- Setup Layout ---
        # Left: Time Series (Takes 50-60% width)
        # Right: Semantic Subplots (2x2 grid)
        fig = plt.figure(figsize=figsize, constrained_layout=True)
        gs = fig.add_gridspec(2, 4) # 2 rows, 4 cols
        
        # Time Series: Spans all rows, first 2 columns
        ax_ts = fig.add_subplot(gs[:, :2])
        duration = len(ts) / sr
        time_axis = np.linspace(0, duration, len(ts))
        
        color = cmap(i % 10)
        ax_ts.plot(time_axis, ts, color=color, linewidth=2)
        ax_ts.set_title(f"Cluster {i} Centroid | {plot_title}", fontsize=14, fontweight='bold')
        ax_ts.set_ylabel("Amplitude")
        ax_ts.set_xlabel("Time (s)")
        ax_ts.margins(x=0)
        ax_ts.set_facecolor('#f9f9f9')
        ax_ts.grid(True, alpha=0.3)

        # Semantic Categories: Spans last 2 columns, arranged in 2x2
        # We plot up to 4 categories
        semantic_axes_indices = [(0, 2), (0, 3), (1, 2), (1, 3)]
        
        for idx, cat in enumerate(categories_to_plot[:4]):
            if cat not in cluster_prompt_means[i] or cluster_prompt_means[i][cat] is None:
                continue
            
            row_idx, col_idx = semantic_axes_indices[idx]
            ax = fig.add_subplot(gs[row_idx, col_idx])
            
            # Retrieve data
            probs = cluster_prompt_means[i][cat]
            labels = category_map.get(cat, [])
            if len(labels) != len(probs): continue 

            # Compute Relevance
            relevance = probs - (1.0 / len(probs))
            
            # Sort Top N
            top_indices = np.argsort(relevance)[-top_n:]
            top_labels_plot = [labels[k] for k in top_indices]
            top_values_plot = relevance[top_indices]
            
            ax.barh(top_labels_plot, top_values_plot, color=color, alpha=0.7)
            ax.set_title(cat, fontsize=10, fontweight='bold')
            ax.axvline(0, color='gray', linewidth=0.8, linestyle='--')
            ax.grid(axis='x', alpha=0.2)
            
        plt.suptitle(f"Cluster {i} Profile", fontsize=18)
        plt.show()



def plot_clusters_scatter(reducer, embeddings, predicted_clusters, figsize=(10,6), title=None):
    """
    Plots a 2D UMAP scatter of clusters and identifies representative samples.
    """
    X_umap = reducer.fit_transform(embeddings)

    unique_clusters = np.unique(predicted_clusters)
    cluster_samples = []  
    centroid_indices = [] 
    samples_per_cluster = 15

    for c in unique_clusters:
        idx_cluster = np.where(predicted_clusters == c)[0]  
        X_c = embeddings[idx_cluster]
        centroid = X_c.mean(axis=0)
        dist_to_centroid = np.linalg.norm(X_c - centroid, axis=1)

        closest_idx_local = np.argsort(dist_to_centroid)[:samples_per_cluster]
        closest_idx_global = idx_cluster[closest_idx_local]
        cluster_samples.append(closest_idx_global)
        centroid_indices.append(closest_idx_global[0])

    plt.figure(figsize=figsize)
    sns.scatterplot(
        x=X_umap[:, 0], 
        y=X_umap[:, 1], 
        hue=predicted_clusters, 
        palette='colorblind', 
        s=100,
        legend='full',
        alpha=0.6
    )

    plot_title = title if title else 'UMAP Projection'
    plt.title(plot_title)
    plt.xlabel("UMAP 1")
    plt.ylabel("UMAP 2")
    plt.legend()
    plt.tight_layout()
    plt.show()

    
    return cluster_samples, centroid_indices






def plot_pca_elbow(embeddings, method_name, max_components=20):
    """
    Plots the Cumulative Explained Variance (PCA Scree Plot) to find the optimal n_components.
    """
    # Handle 3D input (Songs, Time, Features) -> 2D (Total Frames, Features)
    if embeddings.ndim == 3:
        embeddings = embeddings.reshape(-1, embeddings.shape[-1])
    
    # Handle NaNs (PCA cannot handle NaNs)
    mask = ~np.isnan(embeddings).any(axis=1)
    embeddings_valid = embeddings[mask]

    # Limit components to the number of features available
    n_components = min(max_components, embeddings_valid.shape[1])
    
    # Fit PCA once
    pca = PCA(n_components=n_components)
    pca.fit(embeddings_valid)

    # Calculate Cumulative Variance
    cumulative_variance = np.cumsum(pca.explained_variance_ratio_)
    k_values = range(1, n_components + 1)

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.plot(k_values, cumulative_variance, marker='o', color='black', linestyle='-', linewidth=2, alpha=0.8)

    # Add reference lines for standard thresholds (80% and 90% variance)
    plt.axhline(y=0.90, color='gray', linestyle='--', label='90% Var')
    plt.axhline(y=0.80, color='lightgray', linestyle='--', label='80% Var')

    plt.title(f'PCA Variance Explained - {method_name}')
    plt.xlabel('Number of Components')
    plt.ylabel('Cumulative Variance (0.0 - 1.0)')
    plt.xticks(k_values)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.show()

import numpy as np
import matplotlib.pyplot as plt
import stumpy

def visualize_motifs(ts, mp=None, m=100, radius=1.5, search_data=None):
    """
    Visualizes motifs found on a time series (univariate or multivariate).
    Supports hybrid mode (search on search_data, plot on ts).
    """
    m = int(m)
    
    # --- 1. Matrix Profile Computation ---
    if mp is None:
        target_data = np.asarray(search_data if search_data is not None else ts).astype(np.float64)
        print(f"Computing Matrix Profile (m={m}) on data shape {target_data.shape}...")
        
        if target_data.ndim > 1:
            mp_multi = stumpy.mstump(target_data, m)
            mp_array = mp_multi[0] # (P, I) - Takes the dimension-independent P profile
        else:
            mp_array = stumpy.stump(target_data, m)
    else:
        mp_array = np.asarray(mp).astype(np.float64)

    if mp_array.size == 0 or np.all(np.isnan(mp_array[:, 0])):
        print("Error: Invalid Matrix Profile computed.")
        return []

    # --- 2. Motif Identification ---
    min_idx = np.argmin(mp_array[:, 0])
    min_dist = mp_array[min_idx, 0]
    cutoff = min_dist * radius
    
    if search_data is not None and search_data.ndim > 1:
        # For multivariate data, use MP itself as similarity proxy
        distance_profile = mp_array[:, 0]
    else:
        # For univariate data, compute exact Euclidean distance from motif
        query_source = search_data if search_data is not None else ts
        query = query_source[min_idx : min_idx + m]
        distance_profile = stumpy.mass(query, query_source)

    # Filtering matches (non-overlapping)
    match_indices = np.where(distance_profile < cutoff)[0]
    final_indices = []
    if len(match_indices) > 0:
        final_indices = [match_indices[0]]
        for idx in match_indices[1:]:
            if idx > final_indices[-1] + m/2: # Overlap constraint
                final_indices.append(idx)

    print(f"Motif found at index {min_idx} (dist: {min_dist:.4f}). Total matches: {len(final_indices)}")

    # --- 3. Visualization ---
    fig, axs = plt.subplots(2, 1, sharex=True, figsize=(12, 8))
    
    # Plot 1: Time Series & Motifs
    axs[0].plot(ts, color='#333333', lw=1, alpha=0.6, label="Signal")
    axs[0].set_title(f"Motif Discovery (m={m})", fontweight='bold')
    axs[0].set_ylabel("Amplitude")
    
    # Highlight Reference
    axs[0].axvspan(min_idx, min_idx + m, color='black', alpha=0.4, label="Reference Motif")
    
    # Highlight Matches
    colors = plt.cm.viridis(np.linspace(0, 1, len(final_indices)))
    for i, idx in enumerate(final_indices):
        if idx == min_idx: continue
        axs[0].axvspan(idx, idx + m, color=colors[i], alpha=0.4)

    # Plot 2: Profile & Cutoff
    axs[1].plot(distance_profile, color='royalblue', lw=1.5, label="Distance Profile")
    axs[1].axhline(cutoff, color='red', ls='--', alpha=0.7, label=f"Cutoff (r={radius})")
    axs[1].plot(min_idx, distance_profile[min_idx], 'v', color='black', markersize=8)
    
    axs[1].set_title("Similarity Profile", fontweight='bold')
    axs[1].set_ylabel("Distance (Euclidean)")
    axs[1].set_xlabel("Time (Samples)")
    axs[1].legend(loc='upper right')
    
    plt.tight_layout()
    plt.show()
    
    return final_indices

import numpy as np
import matplotlib.pyplot as plt
import stumpy

def visualize_discords(ts, mp=None, m=100, search_data=None):
    """
    Identifies and visualizes the main anomaly (Discord) in a time series.
    The bottom plot shows the Matrix Profile: peaks indicate anomalies.
    """
    m = int(m)
    
    # --- 1. Matrix Profile Computation ---
    if mp is None:
        target_data = np.asarray(search_data if search_data is not None else ts).astype(np.float64)
        print(f"Computing Matrix Profile for Discords (m={m}) on data shape {target_data.shape}...")
        
        if target_data.ndim > 1:
            mp_multi = stumpy.mstump(target_data, m)
            mp_array = mp_multi[0] 
        else:
            mp_array = stumpy.stump(target_data, m)
    else:
        mp_array = np.asarray(mp).astype(np.float64)
    
    if mp_array.size == 0 or np.all(np.isnan(mp_array[:, 0])):
        print("Error: Invalid Matrix Profile computed.")
        return []

    # --- 2. Discord Identification ---
    # The Discord is the point with max distance from its nearest neighbor (Peak in MP)
    max_idx = np.argmax(mp_array[:, 0])
    max_dist = mp_array[max_idx, 0]
    
    print(f"Top Discord found at index {max_idx} (Anomaly Score: {max_dist:.4f})")

    # Use Matrix Profile itself as "Anomaly Score" for visualization
    anomaly_profile = mp_array[:, 0]

    # --- 3. Visualization ---
    fig, axs = plt.subplots(2, 1, sharex=True, figsize=(12, 8))
    
    # Plot 1: Time Series & Discord Region
    axs[0].plot(ts, color='#333333', lw=1, alpha=0.6, label="Signal")
    axs[0].set_title(f"Discord Discovery (m={m})", fontweight='bold')
    axs[0].set_ylabel("Amplitude")
    
    # Highlight Discord
    axs[0].axvspan(max_idx, max_idx + m, color='firebrick', alpha=0.5, label="Top Discord")
    
    # Plot 2: Anomaly Score (Matrix Profile)
    axs[1].plot(anomaly_profile, color='firebrick', lw=1.5, label="Matrix Profile (Anomaly Score)")
    axs[1].plot(max_idx, max_dist, marker='^', color='black', markersize=8, label="Max Anomaly")
    
    axs[1].set_title("Anomaly Score Profile (Peaks = Discords)", fontweight='bold')
    axs[1].set_ylabel("Distance to Nearest Neighbor")
    axs[1].set_xlabel("Time (Samples)")
    axs[1].legend(loc='upper right')
    
    plt.tight_layout()
    plt.show()
    
    return [max_idx]

import librosa
import numpy as np
from scipy.ndimage import gaussian_filter1d

def extract_multivariate_features(target_files, feature_list=['rms', 'spectral_centroid'], original_sr=24000, target_sr=10, target_len=None, sigma=3.0):
    """
    Extracts various audio features from files and returns both smoothed and raw versions.
    Refactored to use extract_features_from_audio internally.
    
    Args:
        target_files (list): List of audio file paths.
        feature_list (list): List of strings (e.g. ['rms', 'spectral_centroid']).
        original_sr (int): Sampling rate for loading.
        target_sr (int): Effective sampling rate (used if target_len is None).
        target_len (int): Fixed output length (if specified, ignores target_sr for sizing).
        sigma (float): Gaussian smoothing applied to each channel.

    Returns:
        dataset_smooth (list): List of smoothed arrays (Time, N_Features).
        dataset_raw (list): List of raw arrays (Time, N_Features).
    """
    dataset_smooth = []
    dataset_raw = []

    for filepath in target_files:
        try:
            y, sr = librosa.load(filepath, sr=original_sr)
            
            # Use the single-array processing function
            # Note: extract_features_from_audio returns (smooth, raw) for single input
            s_smooth, s_raw = extract_features_from_audio(
                y, 
                sr=sr, 
                feature_list=feature_list, 
                target_sr=target_sr, 
                target_len=target_len,
                sigma=sigma
            )
            
            dataset_smooth.append(s_smooth)
            dataset_raw.append(s_raw)
            
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
            
    return dataset_smooth, dataset_raw

def extract_features_from_audio(audio_data, sr, feature_list=['rms', 'spectral_centroid'], target_sr=10, target_len=None, sigma=3.0):
    """
    Extracts audio features from a numpy array using Adaptive Pooling for resizing.
    
    Args:
        audio_data (np.ndarray or list): Single audio array or list of audio arrays.
        sr (int): Input audio sampling rate.
        feature_list (list): List of strings (e.g. ['rms', 'spectral_centroid']).
        target_sr (int): Target sampling rate (used to calculate length if target_len=None).
        target_len (int): Fixed output length in timesteps. If None, uses target_sr * duration.
        sigma (float): Gaussian smoothing before pooling.

    Returns:
        If single input: (smooth_array, raw_array) with shape (Time, N_Features)
        If list input: (list_smooth, list_raw)
    """
    # High resolution hop length for initial extraction (e.g., ~21ms at 24kHz)
    # We extract 'dense' features then pool them down.
    extract_hop = 512 
    
    # Normalize input to list
    single_input = False
    if isinstance(audio_data, np.ndarray) and audio_data.ndim == 1:
        audio_list = [audio_data]
        single_input = True
    else:
        audio_list = audio_data
    
    dataset_smooth = []
    dataset_raw = []

    for y in audio_list:
        song_channels_smooth = []
        song_channels_raw = []
        
        # Calculate target dimensions
        if target_len is not None:
            final_length = target_len
        else:
            duration_sec = len(y) / sr
            final_length = int(duration_sec * target_sr)
            if final_length < 1: final_length = 1

        # 1. Extract High-Res Features
        for f_type in feature_list:
            if f_type == 'rms':
                feat = librosa.feature.rms(y=y, frame_length=extract_hop*2, hop_length=extract_hop)
            elif f_type == 'spectral_centroid':
                feat = librosa.feature.spectral_centroid(y=y, sr=sr, n_fft=extract_hop*4, hop_length=extract_hop)
            elif f_type == 'spectral_bandwidth':
                feat = librosa.feature.spectral_bandwidth(y=y, sr=sr, n_fft=extract_hop*4, hop_length=extract_hop)
            elif f_type == 'spectral_rolloff':
                feat = librosa.feature.spectral_rolloff(y=y, sr=sr, n_fft=extract_hop*4, hop_length=extract_hop)
            elif f_type == 'zero_crossing_rate':
                feat = librosa.feature.zero_crossing_rate(y=y, frame_length=extract_hop*2, hop_length=extract_hop)
            elif f_type == 'mfcc':
                feat = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13, n_fft=extract_hop*4, hop_length=extract_hop) 
            else:
                print(f"Warning: Feature '{f_type}' not supported. Skipping.")
                continue

            # Ensure 2D (channels, time)
            if feat.ndim == 1:
                feat = feat[np.newaxis, :]
            
            # 2. Process each channel
            for channel_idx in range(feat.shape[0]):
                raw_channel = np.nan_to_num(feat[channel_idx].flatten())
                
                # Gaussian Smoothing (optional, but good before pooling to reduce aliasing further)
                if sigma > 0:
                    smooth_channel = gaussian_filter1d(raw_channel, sigma=sigma)
                else:
                    smooth_channel = raw_channel
                
                song_channels_raw.append(smooth_channel)
                song_channels_smooth.append(smooth_channel) # Use smoothed version for pooling input usually?
                # Note: original code returned both. For consistency, let's keep separate lists.
                # Actually, applying AdaptivePooling IS a form of smoothing/resampling.
                # If we want "raw" resampled vs "smooth" resampled, we can apply pooling to both.
        
        # 3. Stack, Convert to Tensor, and Pool
        if song_channels_smooth:
            # Clip to min len if features differ slightly
            min_len = min(len(c) for c in song_channels_smooth)
            channels_smooth = np.stack([c[:min_len] for c in song_channels_smooth]) # (C, T_in)
            channels_raw = np.stack([c[:min_len] for c in song_channels_raw])       # (C, T_in)
            
            # Convert to Tensor (1, C, T)
            t_smooth = torch.tensor(channels_smooth, dtype=torch.float32).unsqueeze(0)
            t_raw = torch.tensor(channels_raw, dtype=torch.float32).unsqueeze(0)
            
            # Apply Adaptive Average Pooling
            # Output: (1, C, final_length)
            pooled_smooth = torch.nn.functional.adaptive_avg_pool1d(t_smooth, final_length)
            pooled_raw = torch.nn.functional.adaptive_avg_pool1d(t_raw, final_length)
            
            # Convert back to Numpy and Transpose to (T, C)
            out_smooth = pooled_smooth.squeeze(0).numpy().T
            out_raw = pooled_raw.squeeze(0).numpy().T
            
            dataset_smooth.append(out_smooth)
            dataset_raw.append(out_raw)
    
    if single_input:
        return dataset_smooth[0], dataset_raw[0]
    return dataset_smooth, dataset_raw

def split_dataset_into_chunks(audio_list, labels, sr, chunk_size_sec, overlap_sec=0):
    """
    Split audio files into fixed-length chunks and assign labels/groups.
    
    Args:
        audio_list: List of audio arrays.
        labels: List of labels corresponding to audio files.
        sr: Sampling rate.
        chunk_size_sec: Chunk duration in seconds.
        overlap_sec: Overlap duration in seconds.
        
    Returns:
        X_chunks (np.array): Segments array.
        y_chunks (np.array): Labels array.
        groups (np.array): Original song indices (for GroupKFold).
    """
    chunk_samples = int(chunk_size_sec * sr)
    hop_samples = int((chunk_size_sec - overlap_sec) * sr)
    
    X_chunks = []
    y_chunks = []
    groups = []
    
    for idx, (y, label) in enumerate(zip(audio_list, labels)):
        if len(y) < chunk_samples:
            continue
            
        for start in range(0, len(y) - chunk_samples + 1, hop_samples):
            segment = y[start : start + chunk_samples]
            X_chunks.append(segment)
            y_chunks.append(label)
            groups.append(idx)  # Important: track original song index
            
    return np.array(X_chunks), np.array(y_chunks), np.array(groups)


def plot_semantic_bars(predicted_clusters, cached_probs, file_paths=None,
                       categories=["Style", "Production", "Vocal", "Tempo", "Density"], 
                       figsize=(16, 12), title_prefix=""):
    """
    Plots grouped bar charts showing the MEAN semantic probability distribution for each cluster.
    Arranges plots in a 2-column grid, ordered by 'Variance Score' (how different the clusters are).
    """
    import math
    
    unique_clusters = sorted(np.unique(predicted_clusters))
    valid_clusters = [c for c in unique_clusters if c != -1]
    
    if not valid_clusters:
        print("No valid clusters found.")
        return

    # Constants mapping
    category_map = {
        "Style": RAP_STYLES,
        "Production": PRODUCTION_LABELS,
        "Vocal": VOCAL_STYLE_LABELS,
        "Tempo": TEMPO_LABELS,
        "Density": DENSITY_LABELS,
    }

    # Prepare robust lookup map: Filename (Basename) -> Probs
    filename_to_probs = {}
    if file_paths is not None:
        for k, v in cached_probs.items():
            if hasattr(v, 'item') and isinstance(v.item(), dict): v = v.item() # Handle numpy wrapper
            if "file_path" in v:
                fname = os.path.basename(v["file_path"])
                filename_to_probs[fname] = v
    
    # --- 1. PRE-COMPUTE DATA & SCORES ---
    category_results = [] # List of tuples: (score, category, dataframe)
    
    for cat in categories:
        labels_text = category_map.get(cat, [])
        if not labels_text: continue
            
        plot_data = [] # List of dicts for DataFrame
        cluster_means = [] # To compute variance score
        
        for c in valid_clusters:
            indices = np.where(predicted_clusters == c)[0]
            
            # Gather probs for this cluster
            probs_list = []
            for idx in indices:
                # STRATEGY 1: Match by Filename
                found = False
                if file_paths is not None and idx < len(file_paths):
                    fpath = file_paths[idx]
                    fname = os.path.basename(fpath)
                    
                    if fname in filename_to_probs:
                        data = filename_to_probs[fname]
                        if cat in data:
                            probs_list.append(data[cat])
                            found = True
                
                # STRATEGY 2: Match by Index
                if not found:
                    i_lookup = int(idx)
                    if i_lookup in cached_probs:
                        # Handle potential numpy wrapper
                        d = cached_probs[i_lookup]
                        if hasattr(d, 'item') and isinstance(d.item(), dict): d = d.item()
                        
                        if cat in d:
                             probs_list.append(d[cat])
            
            if probs_list:
                # Calculate mean vector for this cluster
                mean_vec = np.mean(np.stack(probs_list), axis=0)
                cluster_means.append(mean_vec)
                
                # Add to plot data
                for i, label_name in enumerate(labels_text):
                    # Use shortened label if available
                    display_name = DISPLAY_SHORT_LABELS.get(label_name, label_name)
                    plot_data.append({
                        "Label": display_name,
                        "Mean Probability": mean_vec[i],
                        "Cluster": f"Cluster {c} (n={len(probs_list)})"
                    })
        
        if plot_data and len(cluster_means) > 1:
            # --- SCORE CALCULATION ---
            # We want to prioritize categories where clusters are DIFFERENT.
            # Metric: Mean Euclidean Distance between all pairs of cluster means.
            # Or simpler: Variance of the means across clusters (sum of variances per dimension)
            
            stack_means = np.stack(cluster_means) # (n_clusters, n_labels)
            # Variance across clusters for each label, then sum over labels
            variance_score = np.sum(np.var(stack_means, axis=0))
            
            df_plot = pd.DataFrame(plot_data)
            category_results.append((variance_score, cat, df_plot))
        elif plot_data:
             # Only 1 cluster or limited data, low score
             df_plot = pd.DataFrame(plot_data)
             category_results.append((0.0, cat, df_plot))

    # --- 2. SORT & PLOT ---
    # Sort by score descending (Highest Variance first)
    category_results.sort(key=lambda x: x[0], reverse=True)
    
    n_cats = len(category_results)
    if n_cats == 0:
        print("No semantic data available to plot.")
        return

    n_cols = 2
    n_rows = math.ceil(n_cats / n_cols)
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize, constrained_layout=True)
    if n_cats == 1: axes = np.array([axes]) # Handle single plot case
    axes_flat = axes.flatten() if isinstance(axes, np.ndarray) else [axes]
    
    for i, (score, cat, df_cat) in enumerate(category_results):
        ax = axes_flat[i]
        
        # Horizontal Bar Plot
        sns.barplot(data=df_cat, y="Label", x="Mean Probability", hue="Cluster", 
                    palette="tab10", alpha=0.9, edgecolor="black", ax=ax)
        
        title_text = f"{cat} (Div. Score: {score:.3f})"
        ax.set_title(title_text, fontsize=14, fontweight='bold')
        ax.set_xlabel("Mean Probability")
        ax.set_ylabel("")
        ax.grid(axis='x', linestyle='--', alpha=0.3)
        
        # Optimize Legend: Only show for the first plot to save space, or all if needed?
        # Let's keep it for all but small.
        ax.legend(loc='lower right', fontsize='x-small')

    # Hide unused axes
    for j in range(i + 1, len(axes_flat)):
        axes_flat[j].axis('off')
        
    plt.suptitle(f"{title_prefix} Semantic Profiles (Sorted by Cluster Diversity)", fontsize=18)
    plt.show()
def visualize_semantic_motifs(shp_clf, X, file_paths, cached_probs, 
                              categories=['Style', 'Tempo', 'Production'], 
                              figsize=(15, 5)):
    """
    Visualizes learned shapelets and annotates them with the semantic labels 
    of the best-matching sound sample.
    
    Args:
        shp_clf: Trained LearningShapelets model (tslearn).
        X: Feature matrix used for transform (scaled).
        file_paths: List of file paths corresponding to X rows.
        cached_probs: Dictionary of CLAP probabilities {idx: {cat: probs}}.
        categories: List of categories to annotate.
    """
    import os
    
    # 1. Transform to get distances/locations
    # shapelet_transform returns distances to each shapelet
    predicted_locations = shp_clf.transform(X)
    n_shapelets = len(shp_clf.shapelets_as_time_series_)
    
    # 2. Build quick path lookup from cache
    path_to_probs = {}
    if cached_probs:
        for idx, data in cached_probs.items():
            # Handle possible numpy wrapper
            if hasattr(data, 'item') and isinstance(data.item(), dict):
                data = data.item()
                
            if 'file_path' in data:
                p = str(data['file_path'])
                path_to_probs[p] = data
                path_to_probs[os.path.basename(p)] = data

    plt.figure(figsize=(figsize[0], 4 * n_shapelets))
    
    for i in range(n_shapelets):
        s = shp_clf.shapelets_as_time_series_[i]
        
        # Find best matching sample (min distance)
        best_match_idx = np.argmin(predicted_locations[:, i])
        
        match_path = file_paths[best_match_idx]
        filename = os.path.basename(match_path)
        
        # Build Title String
        title_str = f"Shapelet {i} | Best Match: {filename}\n"
        
        # Retrieve Semantics
        probs_data = path_to_probs.get(match_path) or path_to_probs.get(filename)
        
        if probs_data:
            desc_parts = []
            for cat in categories:
                if cat in probs_data and cat in CATEGORY_TO_LABELS:
                    # Get probs and find max
                    p_vec = probs_data[cat]
                    label_idx = np.argmax(p_vec)
                    label_text = CATEGORY_TO_LABELS[cat][label_idx]
                    
                    # Shorten
                    short_label = DISPLAY_SHORT_LABELS.get(label_text, label_text)
                    mean_p = p_vec[label_idx]
                    
                    desc_parts.append(f"{cat}: {short_label} ({mean_p:.2f})")
            
            if desc_parts:
                title_str += " | ".join(desc_parts)
        else:
            title_str += "(Semantic data not found)"

        # PLOT
        plt.subplot(n_shapelets, 1, i + 1)
        plt.plot(s, linewidth=2)
        plt.title(title_str, fontsize=10)
        plt.grid(True, alpha=0.3)
        
    plt.tight_layout()
    plt.show()



def get_cluster_medoids(X, labels, file_paths=None):
    """
    Finds the 'medoids' (samples closest to the centroid) for each cluster.
    
    Args:
        X: Feature/embedding matrix (N_samples, N_features) or (N, T, F).
        labels: Array of cluster labels.
        file_paths: List of file paths (optional) to retrieve the track name.
        
    Returns:
        medoids_info (dict): {cluster_id: {'index': int, 'file': str, 'distance': float}}
    """
    unique_clusters = np.sort(np.unique(labels))
    medoids = {}
    
    # 1. Handle 3D input (if Time Series) -> Flatten to 2D for Euclidean distance calculation
    if X.ndim == 3:
        # (N, Time, Feat) -> (N, Time*Feat)
        X_flat = X.reshape(X.shape[0], -1)
    else:
        X_flat = X
    print(f"Calculating medoids for {len(unique_clusters)} clusters...")
    for c in unique_clusters:
        if c == -1: continue # Skip noise if present
        
        # A. Identify cluster points
        indices = np.where(labels == c)[0]
        cluster_data = X_flat[indices]
        
        if len(cluster_data) == 0: continue
            
        # B. Calculate Centroid (Mean)
        centroid = np.mean(cluster_data, axis=0)
        
        # C. Find the point closest to the centroid (Medoid)
        distances = np.linalg.norm(cluster_data - centroid, axis=1)
        min_idx_local = np.argmin(distances)
        min_idx_global = indices[min_idx_local]
        
        # D. Retrieve file info
        medoid_file = file_paths[min_idx_global] if file_paths is not None else "N/A"
        medoid_name = os.path.basename(medoid_file) if file_paths is not None else f"Index {min_idx_global}"
        
        medoids[c] = {
            'index': min_idx_global,
            'file': medoid_file,
            'filename': medoid_name,
            'distance': distances[min_idx_local]
        }
        
    return medoids


# =============================================================================
# MOTIF DISCOVERY FUNCTIONS (STUMPY)
# =============================================================================

def prepare_cluster_timeseries(timeseries_list, m, k=None):
    """
    Concatenates a list of time series with np.nan spacers to prevent cross-matches.
    Returns the concatenated series and a mapping to original indices.
    
    Args:
        timeseries_list: List of 1D arrays (time series).
        m: Window size (length of the spacer).
        k: Max number of samples to concatenate (optional).
        
    Returns:
        T_concatenated: The long time series with NaN spacers.
        indices_map: List of tuples (start, end, original_index).
    """
    if k is not None:
        timeseries_list = timeseries_list[:k]
        
    spacer = np.full(m, np.nan)
    T_concatenated = []
    indices_map = []
    current_idx = 0
    
    for i, ts in enumerate(timeseries_list):
        ts = ts.astype(float) # Ensure float for NaNs
        T_concatenated.append(ts)
        
        start = current_idx
        end = current_idx + len(ts)
        indices_map.append((start, end, i))
        
        # Add spacer
        T_concatenated.append(spacer)
        current_idx = end + m
        
    return np.concatenate(T_concatenated), indices_map

def find_and_plot_cluster_motifs(timeseries_list, m, cluster_id, k=None, max_matches=5, distance_threshold=None, title_prefix=""):
    """
    Finds and plots the most frequent motif in a cluster using Matrix Profile.
    
    Args:
        timeseries_list: List of 1D arrays belonging to the cluster.
        m: Motif window size.
        cluster_id: ID of the cluster (for display).
        k: Max samples to use (for speed).
        max_matches: Max number of motif occurrences to plot.
        distance_threshold: Optional distance threshold for matches.
    """
    if not timeseries_list:
        print(f"Cluster {cluster_id} is empty.")
        return

    try:
        import stumpy
    except ImportError:
        print("Error: 'stumpy' library is not installed. Please run 'pip install stumpy'.")
        return

    print(f"[{title_prefix} Cluster {cluster_id}] Preparing data (k={k if k else 'All'})...")
    T, indices_map = prepare_cluster_timeseries(timeseries_list, m, k)
    
    # Clean T for stumpy (it handles NaNs by outputting NaNs in the profile)
    
    print(f"[{title_prefix} Cluster {cluster_id}] Computing Matrix Profile (Length={len(T)})...")
    mp = stumpy.stump(T, m, ignore_trivial=True)
    mp = mp.astype(float) # Ensure float type for NaN checks
    
    # 1. Find the Best Motif (Global Min)
    # mp[:, 0] contains matrix profile values (distances)
    # We ignore NaNs in the profile
    valid_indices = np.where(~np.isnan(mp[:, 0]))[0]
    if len(valid_indices) == 0:
        print("No valid motifs found.")
        return
        
    motif_idx = valid_indices[np.argmin(mp[valid_indices, 0])]
    motif_dist = mp[motif_idx, 0]
    
    print(f"Found Best Motif at index {motif_idx} with distance {motif_dist:.4f}")
    
    # 2. Radius/Similarity Search to find repetitions
    Q = T[motif_idx : motif_idx + m]
    
    # Compute Distance Profile of Q against T
    D = stumpy.mass(Q, T)
    
    # Find matches (peaks in similarity / valleys in distance)
    # Threshold: if not provided, estimate it. 
    if distance_threshold is None:
        distance_threshold = max(motif_dist * 3, 2.0) # Heuristic default
        
    # Get matches indices (valleys)
    from scipy.signal import argrelextrema
    valleys = argrelextrema(D, np.less)[0]
    
    # Filter by threshold
    # Also ignore regions corresponding to spacers (which yield high dist usually, but check for NaNs)
    matches = [v for v in valleys if D[v] < distance_threshold and not np.isnan(D[v])]
    
    # Include the motif itself if not found (sometimes argrelextrema misses exact zero if flat)
    if not any(abs(motif_idx - m_idx) < (m/2) for m_idx in matches):
         matches.append(motif_idx)

    # Sort by distance
    matches = sorted(matches, key=lambda x: D[x])
    
    # Filter overlapping matches
    unique_matches = []
    exclusion_zone = int(m / 2)
    
    for match in matches:
        if not any(abs(match - um) < exclusion_zone for um in unique_matches):
            unique_matches.append(match)
            
    # Limit matches
    top_matches = unique_matches[:max_matches]
    
    print(f"Found {len(unique_matches)} matches within threshold {distance_threshold:.2f}. Showing top {len(top_matches)}.")
    
    # 3. Visualization
    fig, axes = plt.subplots(len(top_matches) + 1, 1, figsize=(10, 2.5 * (len(top_matches) + 1)), sharex=False)
    if len(top_matches) == 0: axes = [axes] 
    if not isinstance(axes, np.ndarray): axes = [axes] # Ensure iterable

    # Plot Query Motif
    axes[0].plot(Q, color='red', linewidth=2, label="Seed Pattern")
    axes[0].set_title(f"Cluster {cluster_id} - Seed Pattern (Index {motif_idx})")
    axes[0].legend(loc="upper right")
    axes[0].grid(True, alpha=0.3)
    
    # Plot Matches
    for i, match_idx in enumerate(top_matches):
        ax = axes[i+1]
        
        # Identify which sample this comes from
        sample_id = -1
        sample_local_idx = -1
        for start, end, orig_idx in indices_map:
            if start <= match_idx < end:
                sample_id = orig_idx
                sample_local_idx = match_idx - start
                break
        
        subseq = T[match_idx : match_idx + m]
        dist = D[match_idx]
        
        ax.plot(subseq, color='blue', alpha=0.7, label=f"Match #{i+1} (D={dist:.2f})")
        ax.set_title(f"Match #{i+1} in Sample {sample_id} @ {sample_local_idx}")
        ax.legend(loc="upper right")
        ax.grid(True, alpha=0.3)
        
    plt.tight_layout()
    plt.show()

def find_and_plot_cluster_discords(timeseries_list, m, cluster_id, k=None, max_discords=3, title_prefix=""):
    """
    Finds and plots the top discords (anomalies) in a cluster using Matrix Profile.
    
    Args:
        timeseries_list: List of 1D arrays belonging to the cluster.
        m: Window size.
        cluster_id: ID of the cluster.
        k: Max samples to use.
        max_discords: Max number of anomalies to plot.
    """
    if not timeseries_list: 
        return

    try:
        import stumpy
    except ImportError:
        print("Error: 'stumpy' is required for motif/discord analysis.")
        return

    print(f"[{title_prefix} Cluster {cluster_id}] Analyzing Discords (k={k if k else 'All'})...")
    T, indices_map = prepare_cluster_timeseries(timeseries_list, m, k)
    
    mp = stumpy.stump(T, m, ignore_trivial=True)
    
    # In MP, Discords are the MAXIMA
    # We sort indices by distance descending
    # Ignore NaNs
    valid_mask = ~np.isnan(mp[:, 0])
    valid_indices = np.where(valid_mask)[0]
    
    if len(valid_indices) == 0:
        print("No valid sequences found.")
        return

    sorted_indices = valid_indices[np.argsort(mp[valid_indices, 0])[::-1]]
    
    # Filter overlapping discords
    unique_discords = []
    exclusion_zone = m
    
    for idx in sorted_indices:
        if not any(abs(idx - u) < exclusion_zone for u in unique_discords):
            unique_discords.append(idx)
            if len(unique_discords) >= max_discords:
                break
                
    # Plot
    fig, axes = plt.subplots(len(unique_discords), 1, figsize=(10, 3 * len(unique_discords)), sharex=False)
    if len(unique_discords) == 0: return
    if not isinstance(axes, np.ndarray): axes = [axes]
    
    for i, discord_idx in enumerate(unique_discords):
        ax = axes[i]
        
        # Identity sample
        sample_id = -1
        sample_local = -1
        for start, end, orig in indices_map:
            if start <= discord_idx < end:
                sample_id = orig
                sample_local = discord_idx - start
                break
                
        subseq = T[discord_idx : discord_idx + m]
        dist = mp[discord_idx, 0]
        
        ax.plot(subseq, color='orange', label=f"Discord #{i+1} (Dist={dist:.2f})")
        ax.set_title(f"Anomaly #{i+1} in Sample {sample_id} @ {sample_local}")
        ax.legend(loc="upper right")
        ax.grid(True, alpha=0.3)
        
    plt.tight_layout()
    plt.show()