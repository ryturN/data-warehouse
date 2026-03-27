import pytz
import jmespath

from datetime import datetime
from loguru import logger
from abc import ABC, abstractmethod

# Helper


class BaseClient(ABC):
    def __init__(self, *args, **kwargs) -> None:
        pass
    
    @abstractmethod
    def consume(self):
        pass

    @abstractmethod
    def produce(self):
        pass
