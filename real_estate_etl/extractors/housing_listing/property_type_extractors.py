import pandas as pd

from utils.log.custom_logger import CustomLogger
from models.config.db_connection_config import DBConnectionConfig
from extractors.abstractions.abstract_extractor import AbstractExtractor

class PropertyTypeExtractor(AbstractExtractor):

    def __init__(
        self, 
        source_df: pd.DataFrame,
        target_db_config: DBConnectionConfig,
        logger: CustomLogger
    ) -> None:
        
        super().__init__(target_db_config=target_db_config)
        
        self._logger = logger
        self._source_df = source_df

    def __get_existing_property_types(self) -> pd.DataFrame:
        """
            A private method that returns the existing property types in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing property types
        """

        return pd.read_sql(sql="SELECT * FROM property_types", con=self._get_db_engine())
    
    def extract(self):
        """
            A method that extracts the property types from a housing_listing file

            returns:
                bool: A boolean flag indicating if the extraction was successfully executed
        """

        self._logger.info("PropertyTypeExtractor.extract Starting Property Type Extraction")

        df_source_property_types = (
            self._source_df[["type"]]
                .drop_duplicates()
                .dropna()
                .rename(columns={"type": "description"})
        )

        df_source_property_types["description"] = df_source_property_types["description"].str.capitalize()

        df_existing_property_types = self.__get_existing_property_types()[["description"]]

        df_new_property_types = df_source_property_types.merge(df_existing_property_types, how="outer", indicator=True)
        df_new_property_types = df_new_property_types[(df_new_property_types["_merge"] == 'left_only')].drop('_merge', axis="columns")
        df_new_property_types = df_new_property_types[["description"]]

        records_to_append = len(df_new_property_types.index)

        if records_to_append > 0:
            self._logger.info(f"PropertyTypesExtractor.extract There are {records_to_append} records to add to the property types.")

            try:
                df_new_property_types.to_sql('property_types', con=self._get_db_engine(), index=False, if_exists='append')
            except:
                self._notifier.notify()
                return False
            
            self._logger.info("The property types were successfully added.")
        else:
            self._logger.info("PropertyTypesExtractor.extract There are no records to add to the property types.")

        return True
