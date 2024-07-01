import pandas as pd

from utils.log.custom_logger import CustomLogger
from models.config.db_connection_config import DBConnectionConfig
from extractors.abstractions.abstract_extractor import AbstractExtractor

class CitiesExtractor(AbstractExtractor):

    def __init__(
        self, 
        source_df: pd.DataFrame,
        target_db_config: DBConnectionConfig,
        logger: CustomLogger
    ) -> None:
        
        super().__init__(target_db_config=target_db_config)
        
        self._logger = logger
        self._source_df = source_df

    def __get_existing_cities(self) -> pd.DataFrame:
        """
            A private method that returns the existing cities in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing cities
        """

        return pd.read_sql(sql="SELECT * FROM cities", con=self._get_db_engine())
    
    def extract(self):
        """
            A method that extracts the cities from a real estate transactions file

            returns:
                bool: A boolean flag indicating if the extraction was successfully executed
        """

        self._logger.info("CitiesExtractor.extract Starting cities Extraction")

        df_source_cities = (
            self._source_df[["Locality"]]
                .drop_duplicates()
                .dropna()
                .rename(columns={"Locality": "name"})
        )

        df_source_cities = df_source_cities[(df_source_cities["name"] != "?")]

        df_source_cities["name"] = df_source_cities["name"].str.capitalize()

        df_existing_cities = self.__get_existing_cities()[["name"]]

        df_new_cities = df_source_cities.merge(df_existing_cities, how="outer", indicator=True)
        df_new_cities = df_new_cities[(df_new_cities["_merge"] == 'left_only')].drop('_merge', axis="columns")
        df_new_cities = df_new_cities[["name"]]

        records_to_append = len(df_new_cities.index)

        if records_to_append > 0:
            self._logger.info(f"CitiesExtractor.extract There are {records_to_append} records to add to the cities.")

            try:
                df_new_cities.to_sql('cities', con=self._get_db_engine(), index=False, if_exists='append')
            except:
                self._notifier.notify()
                return False
            
            self._logger.info("The cities were successfully added.")
        else:
            self._logger.info("CitiesExtractor.extract There are no records to add to the cities.")

        return True
