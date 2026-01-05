import os
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"
import gc
import librosa
import numpy as np
import pandas as pd
import torch
import seaborn as sns
from tqdm import tqdm
from pathlib import Path
from typing import List
from transformers import Wav2Vec2FeatureExtractor, AutoModel, Wav2Vec2Model, ClapModel, ClapProcessor
from tslearn.preprocessing import TimeSeriesResampler
from scipy.ndimage import gaussian_filter1d

class FeatureExtractor:
    def __init__(
        self,
        model_ids: List[str] = ["m-a-p/MERT-v1-95M"],
        sampling_rates: List[int] = [24000],
        device: str = None,
        smoothing_sigma: float = 2.0,
    ):
        self.model_ids = model_ids
        self.sampling_rates = sampling_rates
        self.processors = []
        self.models = []
        self.model_types = []  # Explicitly track model type
        self.smoothing_sigma = smoothing_sigma

        # Device detection
        if device:
            self.device = torch.device(device)
        elif torch.backends.mps.is_available():
            self.device = torch.device("mps")
        elif torch.cuda.is_available():
            self.device = torch.device("cuda")
        else:
            self.device = torch.device("cpu")
        print(f"FeatureExtractor initialized on device: {self.device}")
        
        for model_id in model_ids:
            if model_id == "mfcc":
                self.model_types.append("mfcc")
                self.processors.append(None)
                self.models.append(None)
                continue

            # Determine appropriate dtype for the device
            torch_dtype = torch.float16 if self.device.type in ["cuda", "mps"] else torch.float32

            if "clap" in model_id.lower():
                self.model_types.append("clap")
                self.processors.append(ClapProcessor.from_pretrained(model_id))
                model = ClapModel.from_pretrained(
                    model_id,
                    use_safetensors=True
                )
            else:
                self.model_types.append("transformer")
                self.processors.append(Wav2Vec2FeatureExtractor.from_pretrained(model_id, trust_remote_code=True, use_safetensors=True))
                try:
                    model = AutoModel.from_pretrained(
                        model_id, 
                        trust_remote_code=True, 
                        use_safetensors=True, 
                        torch_dtype=torch_dtype
                    )
                except:
                    model = Wav2Vec2Model.from_pretrained(
                        model_id, 
                        use_safetensors=True, 
                        torch_dtype=torch_dtype
                    )
            
            model.to(self.device)
            self.models.append(model)

    def extract_features(self, file_path: str, required_audio_len: int = None, target_resample_len: int = 500, cache_dir: str = None):
            # Fallback to individual model extraction
            features_list = []
            for idx in range(len(self.models)):
                f = self.extract_model_features(idx, file_path, required_audio_len, target_resample_len, cache_dir)
                features_list.append(f)
                
            return np.concatenate(features_list, axis=1)

    def _pool_features(self, features: np.ndarray, target_len: int) -> np.ndarray:
        """
        Apply Adaptive Average Pooling to resize features to target_len.
        
        Args:
            features: Input features of shape (L, C)
            target_len: Target temporal length
            
        Returns:
            np.ndarray: Resized features of shape (target_len, C)
        """
        # Convert to Tensor suitable for pooling: (Batch, Channels, Length)
        # Input features: (L, C) -> (1, C, L)
        # Note: We perform pooling on CPU to avoid MPS limitations with non-divisible sizes
        features_tensor = torch.tensor(features.transpose(1, 0)).unsqueeze(0).float().cpu()
        
        # Apply Adaptive Average Pooling
        pooled = torch.nn.functional.adaptive_avg_pool1d(features_tensor, target_len)
        
        # Convert back: (1, C, target_len) -> (target_len, C)
        output = pooled.squeeze(0).transpose(0, 1).numpy()
        return output

    def _process_audio_array(self, idx: int, audio_data: np.ndarray, target_resample_len: int):
        """
        Process raw audio waveform through the model and return resampled features.
        This is the core feature extraction logic, separated from loading concerns.
        
        Args:
            idx: Model index
            audio_data: Raw audio waveform as numpy array (already at correct sample rate)
            target_resample_len: Target length for resampling
            
        Returns:
            np.ndarray: Extracted and resampled features
        """
        y = audio_data
        
        
        # Determine target dtype for inference
        target_dtype = torch.float16 if self.device.type in ["cuda", "mps"] else torch.float32

        # Check if this index corresponds to MFCC
        if self.model_ids[idx] == "mfcc":
             # Calculate MFCC (standard settings: 13 coefficients)
             # audio_data is already at self.sampling_rates[idx]
             # librosa.feature.mfcc returns (n_mfcc, T)
             mfcc = librosa.feature.mfcc(y=y, sr=self.sampling_rates[idx], n_mfcc=13)
             # We want (Time, Features) to match transformer output
             raw = mfcc.T
        elif self.model_types[idx] == "clap":
            WINDOW_SEC = 7.0
            HOP_SEC = 2.0  # Increased from 0.5 to 2.0 for speed (4x faster)
            
            window_samples = int(WINDOW_SEC * self.sampling_rates[idx])
            hop_samples = int(HOP_SEC * self.sampling_rates[idx])
            
            embeddings = []
            model_dtype = next(self.models[idx].parameters()).dtype
            
            for i, start in enumerate(range(0, len(y), hop_samples)):
                chunk = y[start:start + window_samples]
                if len(chunk) < self.sampling_rates[idx] * 0.5:
                    break
                
                with torch.no_grad():
                    # Process audio through CLAP processor - using 'audio' instead of deprecated 'audios'
                    inputs = self.processors[idx](audio=chunk, return_tensors="pt", sampling_rate=self.sampling_rates[idx])
                    
                    # Move to device - CLAP expects 'input_features' for audio
                    input_features = inputs["input_features"].to(self.device)
                    
                    # Get audio embeddings using the dedicated method
                    embeds = self.models[idx].get_audio_features(input_features=input_features)
                    embeddings.append(embeds.cpu().squeeze(0).numpy())
                
                # Cleanup
                del inputs, input_features, embeds

                if i % 5 == 0 and torch.backends.mps.is_available():
                    torch.mps.empty_cache()
                    gc.collect()
            
            if embeddings:
                raw = np.array(embeddings)
                if torch.backends.mps.is_available():
                    torch.mps.empty_cache()
            else:
                raw = np.zeros((1, 512))
        else:
            # Sliding window chunking for MERT/Wav2Vec2
            CHUNK_DURATION = 30  # seconds
            CHUNK_SAMPLES = int(self.sampling_rates[idx] * CHUNK_DURATION)

            chunks = []
            if len(y) > CHUNK_SAMPLES:
                for start in range(0, len(y), CHUNK_SAMPLES):
                    end = min(start + CHUNK_SAMPLES, len(y))
                    chunks.append(y[start:end])
            else:
                chunks.append(y)
            
            model_chunk_outputs = []
            
            for chunk in chunks:
                if len(chunk) < 1600:  # Skip tiny chunks (<0.1s) to prevent Conv1D errors
                    continue
                # Tokenize
                inputs = self.processors[idx](
                    chunk, 
                    sampling_rate=self.sampling_rates[idx], 
                    return_tensors="pt", 
                    padding=True
                )
                
                # Move inputs to device and cast explicitly to avoid MPS type errors
                
                new_inputs = {}
                for k, v in inputs.items():
                    v = v.to(self.device)
                    if torch.is_floating_point(v):
                        v = v.to(target_dtype)
                    new_inputs[k] = v
                inputs = new_inputs

                with torch.inference_mode():
                    outputs = self.models[idx](**inputs)
                
                chunk_hidden = outputs.last_hidden_state.detach().cpu().squeeze(0).float().numpy()
                model_chunk_outputs.append(chunk_hidden)
                
                # Aggressive cleanup
                del outputs
                del inputs
                if torch.backends.mps.is_available():
                    torch.mps.empty_cache()

            # Concatenate chunks
            if model_chunk_outputs:
                raw = np.concatenate(model_chunk_outputs, axis=0)
            else:
                # Fallback for empty/failed chunks (should ideally not happen if len check passes)
                # Return zeros with correct shape or handle error
                # Infer hidden size from model config if possible, else default
                hidden_size = self.models[idx].config.hidden_size
                raw = np.zeros((1, hidden_size))


        # Apply Gaussian Smoothing to prevent aliasing before resampling
        if self.smoothing_sigma > 0:
            raw = gaussian_filter1d(raw, sigma=self.smoothing_sigma, axis=0)

        # Replace TimeSeriesResampler with Adaptive Pooling
        final_features = self._pool_features(raw, target_resample_len)

        return final_features.astype(np.float32)

    def _load_from_cache(self, idx: int, file_path: str, target_resample_len: int, cache_dir: str):
        """
        Try to load features from cache.
        
        Returns:
            tuple: (features, cache_path) if cache hit, (None, cache_path) if cache miss
        """
        filename = Path(file_path).stem
        safe_id = self.model_ids[idx].replace("/", "_") if self.model_ids[idx] else "mfcc"
        cache_name = f"{filename}_{safe_id}_len{target_resample_len}.npz"
        cache_path = None
        
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
            cache_path = os.path.join(cache_dir, cache_name)
            
            if os.path.exists(cache_path):
                try:
                    loaded = np.load(cache_path)
                    raw_from_cache = loaded['features'] if isinstance(loaded, np.lib.npyio.NpzFile) else loaded
                    
                    # Validate dimensions against model config if possible
                    try:
                        expected_dim = self.models[idx].config.hidden_size
                        if raw_from_cache.shape[-1] != expected_dim:
                            # Note: This check might fail if cache already has resampled length != dim
                            # But raw_from_cache in previous implementation was raw (L, C) or resampled (TargetL, C)?
                            # Previous implementation saved `resampled_3d[0]` which is (TargetL, C).
                            # So this check only makes sense if we wanted to enforce C dimension.
                            pass
                    except AttributeError:
                        pass
                    
                    # If loaded shape matches target length, return it directly
                    if raw_from_cache.shape[0] == target_resample_len:
                        return raw_from_cache.astype(np.float32), cache_path
                    
                    # Otherwise, resize (e.g. if loaded raw sequence, but we usually save resampled)
                    # Assuming we cache the RESAMPLED version basically always in this pipeline.
                    # But if we want to be safe:
                    final_features = self._pool_features(raw_from_cache, target_resample_len)

                    # Update cache with new length? Maybe not, just return
                    return final_features.astype(np.float32), cache_path
                except Exception as e:
                    pass

        return None, cache_path

    def extract_model_features(self, idx: int, file_path: str, required_audio_len: int, target_resample_len: int, cache_dir: str):
        """Extract features for a single model from file, using cache if available."""
        # Try cache first
        cached_features, cache_path = self._load_from_cache(idx, file_path, target_resample_len, cache_dir)
        
        if cached_features is not None:
            return cached_features
        
        # Load audio from file
        y, _ = librosa.load(file_path, sr=self.sampling_rates[idx])
        
        # Process audio through model
        final_features = self._process_audio_array(idx, y, target_resample_len)

        # Save to cache
        if cache_path:
            np.savez_compressed(cache_path, features=final_features.astype(np.float16))

        return final_features

    def extract_features_from_np(self, audio_data: np.ndarray, current_sr: int, target_resample_len: int = 500):
        """
        Extract features from a numpy array (already loaded with librosa).
        Automatically handles resampling if current_sr doesn't match model requirements.
        
        Args:
            audio_data: Raw audio waveform as numpy array
            current_sr: Sample rate of the provided audio_data
            target_resample_len: Target length for resampling (default 500)
            
        Returns:
            np.ndarray: Concatenated features from all models
        """
        features_list = []
        for idx in range(len(self.models)):
            target_sr = self.sampling_rates[idx]
            
            # Resample if needed
            if current_sr != target_sr:
                audio_resampled = librosa.resample(audio_data, orig_sr=current_sr, target_sr=target_sr)
            else:
                audio_resampled = audio_data
                
            f = self._process_audio_array(idx, audio_resampled, target_resample_len)
            features_list.append(f)
            
        return np.concatenate(features_list, axis=1)

    # extract_features_batch removed as per user request
    
if __name__ == "__main__":
    
    # Initialize CLAP Extractor
    print("Initializing CLAP Extractor on CPU...")
    extractor = FeatureExtractor(
        model_ids=["laion/clap-htsat-unfused"],
        sampling_rates=[48000],
        device="mps"  
    )

    MP3_FOLDER = "/Users/lorenzoallegrini/Downloads/fedez_fibra"
    DATASETS_FOLDER = "../datasets"


    ARTIST_MAPPING = {
        "07024718": "Fedez",
        "25707984": "Fabri Fibra"
    }

    def get_artist_from_filename(filename):
        try:
            # Note: This function works only on the filename, not the full path
            artist_id = filename.split(' - ')[0].replace('ART', '')
            return ARTIST_MAPPING.get(artist_id, "Unknown")
        except:
            return "Unknown"

    print(os.listdir(MP3_FOLDER))

    filenames = [f for f in os.listdir(MP3_FOLDER) if f.endswith('.mp3')]
    print(f"Found {len(filenames)} MP3 files.")


    fedez_files = [
        os.path.join(MP3_FOLDER, f)
        for f in filenames
        if get_artist_from_filename(f) == "Fedez"
    ]

    fibra_files = [
        os.path.join(MP3_FOLDER, f)
        for f in filenames
        if get_artist_from_filename(f) == "Fabri Fibra"
    ]

    all_files = fedez_files + fibra_files
    
    if not all_files:
        print(f"No audio files found in {MP3_FOLDER}. Please check path.")
    else:
        print(f"Found {len(all_files)} files to test.")
        
        CACHE_DIR = "./extracted_features_clap"
        subset = all_files[:200] # Takes from 204 onwards
        subset_reversed = subset[::-1] # Reverses them (last becomes first)

        for i, file_path in enumerate(tqdm(subset_reversed, desc="Processing Reverse")):
            print(f"Processing {i+1}/{len(all_files)}: {file_path}")
            try:
                # This will use per-model caching inside extract_model_features
                feats = extractor.extract_features(
                    file_path=str(file_path),
                    target_resample_len=500,
                    cache_dir=CACHE_DIR
                )
                print(f"  -> Output shape: {feats.shape}")
            except Exception as e:
                print(f"  -> Error: {e}")
                
        print("\nTest complete!")