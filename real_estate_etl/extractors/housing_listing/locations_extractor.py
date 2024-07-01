import pandas as pd

from utils.log.custom_logger import CustomLogger
from models.config.db_connection_config import DBConnectionConfig
from extractors.abstractions.abstract_extractor import AbstractExtractor

class LocationsExtractor(AbstractExtractor):

    def __init__(
        self, 
        source_df: pd.DataFrame,
        target_db_config: DBConnectionConfig,
        logger: CustomLogger
    ) -> None:
        
        super().__init__(target_db_config=target_db_config)
        
        self._logger = logger
        self._source_df = source_df


    def __get_existing_regions(self) -> pd.DataFrame:
        """
            A private method that returns the existing regions in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing regions
        """

        return pd.read_sql(sql="SELECT * FROM regions", con=self._get_db_engine())
    

    def __get_existing_states(self) -> pd.DataFrame:
        """
            A private method that returns the existing states in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing states
        """

        return pd.read_sql(sql="SELECT * FROM states", con=self._get_db_engine())
    

    def __get_existing_locations(self) -> pd.DataFrame:
        """
            A private method that returns the existing locations in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing locations
        """

        return pd.read_sql(sql="SELECT * FROM locations", con=self._get_db_engine())
    
    def extract(self):
        """
            A method that extracts the locations from a housing_listing file

            returns:
                bool: A boolean flag indicating if the extraction was successfully executed
        """  

        self._logger.info("LocationsExtractor.extract Starting locations Extraction")

        df_source_locations = (
            self._source_df[["region", "region_url", "state"]]
                .drop_duplicates()
                .dropna()
        )

        df_source_locations["region"] = df_source_locations["region"].str.lower()
        df_source_locations["region_url"] = df_source_locations["region_url"].str.lower()
        df_source_locations["state"] = df_source_locations["state"].str.lower()

        df_existing_regions = self.__get_existing_regions()[["region_id","description", "region_url"]]
        df_existing_regions["description"] = df_existing_regions["description"].str.lower()
        df_existing_regions["region_url"] = df_existing_regions["region_url"].str.lower()

        df_existing_states = self.__get_existing_states()[["state_id","acronym"]]
        df_existing_states["acronym"] = df_existing_states["acronym"].str.lower()

        df_source_locations = df_source_locations.merge(df_existing_regions, how="left", left_on=["region", "region_url"], right_on=["description","region_url"])
        df_source_locations = df_source_locations.merge(df_existing_states, how="left", left_on="state", right_on="acronym")

        df_source_locations = df_source_locations[["region_id", "state_id"]]

        df_existing_locations = self.__get_existing_locations()[["region_id", "state_id"]]

        df_new_locations = df_source_locations.merge(df_existing_locations, how="outer", indicator=True)
        df_new_locations = df_new_locations[(df_new_locations["_merge"] == 'left_only')].drop('_merge', axis="columns")
        df_new_locations = df_new_locations[["region_id", "state_id"]]

        records_to_append = len(df_new_locations.index)

        if records_to_append > 0:
            self._logger.info(f"LocationsExtractor.extract There are {records_to_append} records to add to the locations.")

            try:
                df_new_locations.to_sql('locations', con=self._get_db_engine(), index=False, if_exists='append')
            except:
                self._notifier.notify()
                return False
            
            self._logger.info("The locations were successfully added.")
        else:
            self._logger.info("LocationsExtractor.extract There are no records to add to the locations.")

        return True