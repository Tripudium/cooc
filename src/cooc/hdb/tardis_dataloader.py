from pathlib import Path
import logging

# Local imports    
from cooc.hdb.base import DataLoader
from cooc.hdb.registry import register_dataset

logger = logging.getLogger(__name__)
TARDIS_DATA_PATH = Path(__file__).parent.parent.parent.parent / "data" / "tardis"       

@register_dataset("tardis")
class TardisData(DataLoader):
    """
    Dataloader for Tardis data (obtained via Terank)
    """
    def __init__(self, root: str | Path = TARDIS_DATA_PATH):
        super().__init__(root)