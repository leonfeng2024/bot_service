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
        self.init_model()

    def init_model(self) -> None:
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

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