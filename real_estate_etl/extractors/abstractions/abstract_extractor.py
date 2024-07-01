import sqlalchemy as sa
import pandas as pd

from notification.pager_duty_notifier import PagerDutyNotifier
from models.config.db_connection_config import DBConnectionConfig

class AbstractExtractor():

    def __init__(self, target_db_config: DBConnectionConfig):
        self._notifier = PagerDutyNotifier()
        
        self._target_db_config = target_db_config
        self._db_engine = None

    def _get_db_engine(self) -> sa.Engine:
        """
            A protected method that returns a SQL Alachemy PostgreSQL Connection

            returns:
                sa.Engine: A sqlalchemy db engine object
        """

        if not self._db_engine:
            self._db_engine = sa.create_engine(
                url=f"postgresql://{self._target_db_config.db_username}:{self._target_db_config.db_password}@{self._target_db_config.db_host}:{self._target_db_config.db_port}/{self._target_db_config.db_name}"
            )    

        return self._db_engine
 

    def _read_csv(self, file_path: str, sep: str = ","):
        """
            A protected method that returns a pandas data frame for the given file
        """
        
        return pd.read_csv(filepath_or_buffer=file_path, sep=sep)

    def extract(self):
        raise NotImplementedError("The method extract was not implemented")