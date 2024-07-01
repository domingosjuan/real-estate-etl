import pandas as pd

from utils.log.custom_logger import CustomLogger
from models.config.db_connection_config import DBConnectionConfig
from extractors.abstractions.abstract_extractor import AbstractExtractor

class ParkingOptionsExtractor(AbstractExtractor):

    def __init__(
        self, 
        source_df: pd.DataFrame,
        target_db_config: DBConnectionConfig,
        logger: CustomLogger
    ) -> None:
        
        super().__init__(target_db_config=target_db_config)
        
        self._logger = logger
        self._source_df = source_df

    def __get_existing_parking_options(self) -> pd.DataFrame:
        """
            A private method that returns the existing parking options in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing parking options
        """

        return pd.read_sql(sql="SELECT * FROM parking_options", con=self._get_db_engine())
    
    def extract(self):
        """
            A method that extracts the parking options from a housing_listing file

            returns:
                bool: A boolean flag indicating if the extraction was successfully executed
        """

        self._logger.info("ParkingOptionsExtractor.extract Starting parking Options Extraction")

        df_source_parking_options = (
            self._source_df[["parking_options"]]
                .drop_duplicates()
                .dropna()
                .rename(columns={"parking_options": "description"})
        )

        df_source_parking_options["description"] = df_source_parking_options["description"].str.capitalize()

        df_existing_parking_options = self.__get_existing_parking_options()[["description"]]

        df_new_parking_options = df_source_parking_options.merge(df_existing_parking_options, how="outer", indicator=True)
        df_new_parking_options = df_new_parking_options[(df_new_parking_options["_merge"] == 'left_only')].drop('_merge', axis="columns")
        df_new_parking_options = df_new_parking_options[["description"]]


        records_to_append = len(df_new_parking_options.index)

        if records_to_append > 0:
            self._logger.info(f"ParkingOptionsExtractor.extract There are {records_to_append} records to add to the parking options.")

            try:
                df_new_parking_options.to_sql('parking_options', con=self._get_db_engine(), index=False, if_exists='append')
            except:
                self._notifier.notify()
                return False
            
            self._logger.info("The parking options were successfully added.")
        else:
            self._logger.info("ParkingOptionsExtractor.extract There are no records to add to the parking options.")

        return True