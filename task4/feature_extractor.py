import os
from pathlib import Path
import torch
from transformers import Wav2Vec2FeatureExtractor, AutoModel
from sklearn.decomposition import PCA
import librosa
import numpy as np
from typing import List
import pandas as pd
import seaborn as sns

import os
import numpy as np
import librosa
import torch
from pathlib import Path
from typing import List
from transformers import Wav2Vec2FeatureExtractor, AutoModel, Wav2Vec2Model
from tslearn.preprocessing import TimeSeriesResampler

class FeatureExtractor:
    def __init__(
        self,
        model_ids: List[str] = ["m-a-p/MERT-v1-95M", "facebook/wav2vec2-base"],
        sampling_rates: List[int] = [24000, 16000],
    ):
        self.sampling_rates = sampling_rates
        self.model_ids = model_ids
        self.processors = []
        self.models = []
        
        for model_id in model_ids:
            self.processors.append(Wav2Vec2FeatureExtractor.from_pretrained(model_id, trust_remote_code=True))
            try:
                model = AutoModel.from_pretrained(model_id, trust_remote_code=True)
            except:
                model = Wav2Vec2Model.from_pretrained(model_id)
            self.models.append(model)

    def extract_features(self, file_path: str, required_audio_len: int = None, target_resample_len: int = 500, cache_dir: str = None):
        features_list = []
        for idx in range(len(self.models)):
            f = self.extract_model_features(idx, file_path, required_audio_len, target_resample_len, cache_dir)
            features_list.append(f)
            
        return np.concatenate(features_list, axis=1)

    def extract_model_features(self, idx: int, file_path: str, required_audio_len: int, target_resample_len: int, cache_dir: str):
            filename = Path(file_path).stem
            safe_id = self.model_ids[idx].replace("/", "_")
            cache_name = f"{filename}_{safe_id}_len{target_resample_len}.npz"
            
            if cache_dir:
                os.makedirs(cache_dir, exist_ok=True)
                cache_path = os.path.join(cache_dir, cache_name)
                
                # --- MODIFICA RICHIESTA: Se esiste, carica -> resample -> salva di nuovo ---
                if os.path.exists(cache_path):
                    try:
                        # 1. Carica quello che c'è (gestisce sia npz che npy vecchi)
                        loaded = np.load(cache_path)
                        # Se è un archivio .npz prendi 'features', altrimenti è l'array diretto
                        raw_from_cache = loaded['features'] if isinstance(loaded, np.lib.npyio.NpzFile) else loaded

                        # 2. Resampling Forzato (anche se era già fatto, lo rifà per sicurezza)
                        resampler = TimeSeriesResampler(sz=target_resample_len)
                        # Aggiunge dimensione batch (1, Time, Feat) per tslearn
                        resampled_3d = resampler.fit_transform(raw_from_cache[np.newaxis, :, :])
                        final_features = resampled_3d[0]

                        # 3. Salva di nuovo (Sovrascrive col formato corretto compresso)
                        np.savez_compressed(cache_path, features=final_features.astype(np.float16))
                        print(f"Refreshed & Resaved from cache: {filename} ({safe_id})")

                        return final_features.astype(np.float32)
                    except Exception as e:
                        print(f"Errore rigenerazione cache per {filename}, ricalcolo da zero: {e}")
                        # Se fallisce, prosegue sotto e lo rifà dall'audio

            # --- FLUSSO STANDARD (Se non c'era cache o è fallita) ---
            y, _ = librosa.load(file_path, sr=self.sampling_rates[idx])
            
            if required_audio_len:
                if len(y) > required_audio_len:
                    start = (len(y) - required_audio_len) // 2
                    y = y[start : start + required_audio_len]
                elif len(y) < required_audio_len:
                    y = np.pad(y, (0, required_audio_len - len(y)), mode='constant')

            inputs = self.processors[idx](y, sampling_rate=self.sampling_rates[idx], return_tensors="pt")
            with torch.no_grad():
                outputs = self.models[idx](**inputs)
            
            raw = outputs.last_hidden_state.squeeze(0).numpy()

            # Resampling immediato
            resampler = TimeSeriesResampler(sz=target_resample_len)
            resampled_3d = resampler.fit_transform(raw[np.newaxis, :, :])
            final_features = resampled_3d[0]

            if cache_dir:
                # Salva compresso
                np.savez_compressed(cache_path, features=final_features.astype(np.float16))
                print(f"Saved new: {filename} ({safe_id})")

            return final_features.astype(np.float32)
