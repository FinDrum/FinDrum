from abc import ABC, abstractmethod
import pandas as pd

class Processor(ABC):
    @abstractmethod
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
