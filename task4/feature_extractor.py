import transformers
from transformers import Wav2Vec2FeatureExtractor, AutoModel
from sklearn.decomposition import PCA
import librosa
import numpy as np

class FeatureExtractor:
    def __init__(self, model_id: str = "m-a-p/MERT-v1-95M", pca_components: int = 20):
        self.processor = Wav2Vec2FeatureExtractor.from_pretrained(model_id, trust_remote_code=True)
        self.model = AutoModel.from_pretrained(model_id, trust_remote_code=True)
        self.pca = PCA(n_components=pca_components)

    def extract_features(self, file_path: str, sr: int = 24000, required_len: int = None):
        y, sr = librosa.load(file_path, sr=sr)
        original_len = len(y)
        
        if required_len is not None:
            if len(y) > required_len:
                # Troncamento intelligente: estrae la porzione centrale
                # Evita intro/outro che spesso contengono silenzio o fade
                start = (len(y) - required_len) // 2
                y = y[start:start + required_len]
                print(f"Truncated: {original_len} -> {len(y)} (centro)")
            elif len(y) < required_len:
                # Padding con zeri alla fine
                y = np.pad(y, (0, required_len - len(y)), mode='constant', constant_values=0)
                print(f"Padded: {original_len} -> {len(y)}")
        
        inputs = self.processor(y, sampling_rate=sr, return_tensors="pt", padding=True)
        outputs = self.model(**inputs)
        features = outputs.last_hidden_state
        print(features.shape)
        return features