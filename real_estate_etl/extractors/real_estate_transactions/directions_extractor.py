import pandas as pd

from utils.log.custom_logger import CustomLogger
from models.config.db_connection_config import DBConnectionConfig
from extractors.abstractions.abstract_extractor import AbstractExtractor

class DirectionsExtractor(AbstractExtractor):

    def __init__(
        self, 
        source_df: pd.DataFrame,
        target_db_config: DBConnectionConfig,
        logger: CustomLogger
    ) -> None:
        
        super().__init__(target_db_config=target_db_config)
        
        self._logger = logger
        self._source_df = source_df

    def __get_existing_directions(self) -> pd.DataFrame:
        """
            A private method that returns the existing directions in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing directions
        """

        return pd.read_sql(sql="SELECT * FROM directions", con=self._get_db_engine())
    
    def extract(self):
        """
            A method that extracts the directions from a real estate transactions file

            returns:
                bool: A boolean flag indicating if the extraction was successfully executed
        """

        self._logger.info("DirectionsExtractor.extract Starting directions Extraction")

        df_source_directions = (
            self._source_df[["Face"]]
                .drop_duplicates()
                .dropna()
                .rename(columns={"Face": "description"})
        )

        df_source_directions = df_source_directions[(df_source_directions["description"] != "?")]

        df_source_directions["description"] = df_source_directions["description"].str.capitalize()

        df_existing_directions = self.__get_existing_directions()[["description"]]

        df_new_directions = df_source_directions.merge(df_existing_directions, how="outer", indicator=True)
        df_new_directions = df_new_directions[(df_new_directions["_merge"] == 'left_only')].drop('_merge', axis="columns")
        df_new_directions = df_new_directions[["description"]]

        records_to_append = len(df_new_directions.index)

        if records_to_append > 0:
            self._logger.info(f"DirectionsExtractor.extract There are {records_to_append} records to add to the directions.")

            try:
                df_new_directions.to_sql('directions', con=self._get_db_engine(), index=False, if_exists='append')
            except:
                self._notifier.notify()
                return False
            
            self._logger.info("The directions were successfully added.")
        else:
            self._logger.info("DirectionsExtractor.extract There are no records to add to the directions.")

        return True