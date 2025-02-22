from pathlib import Path
import logging

# Local imports    
from .base import DataLoader
from .registry import register_dataset

logger = logging.getLogger(__name__)
TRNK_DATA_PATH = Path(__file__).parent.parent.parent / "data" / "trnk"       

@register_dataset("trnk")
class TrnkData(DataLoader):
    """
    Dataloader for Terank data.
    """
    def __init__(self, root: str | Path = TRNK_DATA_PATH):
        super().__init__(root)

    def process(self) -> None:
        """
        If data is not available, it can't be processed automatically.
        """
        return None