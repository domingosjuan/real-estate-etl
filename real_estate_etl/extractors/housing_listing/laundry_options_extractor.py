import pandas as pd

from utils.log.custom_logger import CustomLogger
from models.config.db_connection_config import DBConnectionConfig
from extractors.abstractions.abstract_extractor import AbstractExtractor


class LaundryOptionsExtractor(AbstractExtractor):

    def __init__(
        self, 
        source_df: pd.DataFrame,
        target_db_config: DBConnectionConfig,
        logger: CustomLogger
    ) -> None:
        
        super().__init__(target_db_config=target_db_config)
        
        self._logger = logger
        self._source_df = source_df

    def __get_existing_laundry_options(self) -> pd.DataFrame:
        """
            A private method that returns the existing laundry options in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing laundry options
        """

        return pd.read_sql(sql="SELECT * FROM laundry_options", con=self._get_db_engine())

    def extract(self):
        """
            A method that extracts the laundry options from a housing_listing file

            returns:
                bool: A boolean flag indicating if the extraction was successfully executed
        """

        self._logger.info("LaundryOptionsExtractor.extract Starting Laundry Options Extraction")

        df_source_laundry_options = (
            self._source_df[["laundry_options"]]
                .drop_duplicates()
                .dropna()
                .rename(columns={"laundry_options": "description"})
        )

        df_source_laundry_options["description"] = df_source_laundry_options["description"].str.capitalize()

        df_existing_laundry_options = self.__get_existing_laundry_options()[["description"]]

        df_new_laundry_options = df_source_laundry_options.merge(df_existing_laundry_options, how="outer", indicator=True)
        df_new_laundry_options = df_new_laundry_options[(df_new_laundry_options["_merge"] == 'left_only')].drop('_merge', axis="columns")
        df_new_laundry_options = df_new_laundry_options[["description"]]

        records_to_append = len(df_new_laundry_options.index)

        if records_to_append > 0:
            self._logger.info(f"LaundryOptionsExtractor.extract There are {records_to_append} records to add to the laundry options.")

            try:
                df_new_laundry_options.to_sql('laundry_options', con=self._get_db_engine(), index=False, if_exists='append')
            except:
                self._notifier.notify()
                return False
            
            self._logger.info("The laundry options were successfully added.")
        else:
            self._logger.info("LaundryOptionsExtractor.extract There are no records to add to the laundry options.")

        return True
