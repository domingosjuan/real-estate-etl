import pandas as pd

from utils.log.custom_logger import CustomLogger
from models.config.db_connection_config import DBConnectionConfig
from extractors.abstractions.abstract_extractor import AbstractExtractor

class RegionsExtractor(AbstractExtractor):

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
    
    def extract(self):
        """
            A method that extracts the regions from a housing_listing file

            returns:
                bool: A boolean flag indicating if the extraction was successfully executed
        """

        self._logger.info("RegionsExtractor.extract Starting Regions Extraction")

        df_source_regions = (
            self._source_df[["region", "region_url"]]
                .drop_duplicates()
                .dropna()
                .rename(columns={"region": "description"})
        )

        df_source_regions["description"] = df_source_regions["description"].str.capitalize()

        df_existing_regions = self.__get_existing_regions()[["description", "region_url"]]

        df_new_regions = df_source_regions.merge(df_existing_regions, how="outer", indicator=True)
        df_new_regions = df_new_regions[(df_new_regions["_merge"] == 'left_only')].drop('_merge', axis="columns")
        df_new_regions = df_new_regions[["description", "region_url"]]

        records_to_append = len(df_new_regions.index)

        if records_to_append > 0:
            self._logger.info(f"RegionsExtractor.extract There are {records_to_append} records to add to the regions.")

            try:
                df_new_regions.to_sql('regions', con=self._get_db_engine(), index=False, if_exists='append')
            except:
                self._notifier.notify()
                return False
            
            self._logger.info("The regions were successfully added.")
        else:
            self._logger.info("RegionsExtractor.extract There are no records to add to the regions.")

        return True