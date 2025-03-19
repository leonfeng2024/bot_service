from typing import Optional, List
from transformers import AutoModel, AutoTokenizer, PreTrainedModel
import torch
import os
from service.utils import singleton

@singleton
class EmbeddingService:
    def __init__(self):
        self.model: Optional[PreTrainedModel] = None
        self.tokenizer: Optional[AutoTokenizer] = None
        self.model_name = "BAAI/bge-m3"
        self.cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cache")
        self.local_model_dir = os.path.join(self.cache_dir, "bge-m3")
        self.init_model()

    def init_model(self) -> None:
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

        # Check if local model files exist
        local_model_exists = os.path.exists(self.local_model_dir) and os.path.isfile(os.path.join(self.local_model_dir, "pytorch_model.bin"))
        
        try:
            if local_model_exists:
                print(f"Loading model from local directory: {self.local_model_dir}")
                self.tokenizer = AutoTokenizer.from_pretrained(
                    self.local_model_dir,
                    local_files_only=True
                )
                self.model = AutoModel.from_pretrained(
                    self.local_model_dir,
                    local_files_only=True
                )
            else:
                print(f"Local model not found, downloading from {self.model_name} to {self.cache_dir}")
                self.tokenizer = AutoTokenizer.from_pretrained(
                    self.model_name,
                    cache_dir=self.cache_dir
                )
                self.model = AutoModel.from_pretrained(
                    self.model_name,
                    cache_dir=self.cache_dir
                )
        except Exception as e:
            print(f"Error loading model: {str(e)}")
            print("Falling back to remote model download")
            self.tokenizer = AutoTokenizer.from_pretrained(
                self.model_name,
                cache_dir=self.cache_dir
            )
            self.model = AutoModel.from_pretrained(
                self.model_name,
                cache_dir=self.cache_dir
            )
            
        self.model.eval()

    async def get_embedding(self, text: str) -> List[float]:
        if not self.model or not self.tokenizer:
            raise RuntimeError("Embedding model not initialized")

        inputs = self.tokenizer(
            text,
            padding=True,
            truncation=True,
            return_tensors="pt"
        )

        with torch.no_grad():
            outputs = self.model(**inputs)
            embeddings = outputs.last_hidden_state.mean(dim=1)

        return embeddings[0].tolist()