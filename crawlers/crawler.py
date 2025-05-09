from abc import ABC, abstractmethod
import pandas as pd

class Crawler(ABC):
    @abstractmethod
    def fetch(self, **kwargs) -> pd.DataFrame:
        pass