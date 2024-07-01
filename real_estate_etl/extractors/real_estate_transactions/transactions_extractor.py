import pandas as pd

from utils.log.custom_logger import CustomLogger
from models.config.db_connection_config import DBConnectionConfig
from extractors.abstractions.abstract_extractor import AbstractExtractor

class TransactionsExtractor(AbstractExtractor):

    def __init__(
        self, 
        source_df: pd.DataFrame,
        target_db_config: DBConnectionConfig,
        logger: CustomLogger
    ) -> None:
        
        super().__init__(target_db_config=target_db_config)
        
        self._logger = logger
        self._source_df = source_df


    def __get_existing_building_types(self) -> pd.DataFrame:
        """
            A private method that returns the existing building types in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing building types
        """

        return pd.read_sql(sql="SELECT building_type_id, LOWER(description) AS Property FROM building_types", con=self._get_db_engine())


    def __get_existing_cities(self) -> pd.DataFrame:
        """
            A private method that returns the existing cities in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing cities
        """

        return pd.read_sql(sql="SELECT city_id, LOWER(name) AS Locality FROM cities", con=self._get_db_engine())
    

    def __get_existing_directions(self) -> pd.DataFrame:
        """
            A private method that returns the existing directions in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing directions
        """

        return pd.read_sql(sql="SELECT direction_id, LOWER(description) AS Face FROM directions", con=self._get_db_engine())
    

    def __get_existing_property_types(self) -> pd.DataFrame:
        """
            A private method that returns the existing property types in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing property types
        """

        return pd.read_sql(sql="SELECT property_type_id, LOWER(description) AS Residential FROM property_types", con=self._get_db_engine())
    

    def __get_existing_transactions(self) -> pd.DataFrame:
        """
            A private method that returns the existing property transactions in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing property transactions
        """

        return pd.read_sql(sql="SELECT * FROM transactions", con=self._get_db_engine())
    

    def extract(self):
        """
            A method that extracts the the property transactions from a real_estate_transactions file

            returns:
                bool: A boolean flag indicating if the extraction was successfully executed
        """

        self._logger.info("TransactionsExtractor.extract Starting Property Transactions Extraction")

        df_source_property_transactions = (
            self._source_df
                .drop_duplicates()
                .dropna()
        )

        df_source_property_transactions["Locality"] = df_source_property_transactions["Locality"].str.lower()
        df_source_property_transactions["Property"] = df_source_property_transactions["Property"].str.lower()
        df_source_property_transactions["Residential"] = df_source_property_transactions["Residential"].str.lower()
        df_source_property_transactions["Face"] = df_source_property_transactions["Face"].str.lower()

        df_existing_cities = self.__get_existing_cities()

        df_source_property_transactions = df_source_property_transactions.merge(
            df_existing_cities, 
            how="left", 
            left_on="Locality",
            right_on="locality",
            indicator=False
        )

        df_existing_building_types = self.__get_existing_building_types()

        df_source_property_transactions = df_source_property_transactions.merge(
            df_existing_building_types, 
            how="left", 
            left_on="Property",
            right_on="property",
            indicator=False
        )

        df_existing_property_types = self.__get_existing_property_types()

        df_source_property_transactions = df_source_property_transactions.merge(
            df_existing_property_types, 
            how="left", 
            left_on="Residential",
            right_on="residential",
            indicator=False
        )

        df_get_existing_directions = self.__get_existing_directions()

        df_source_property_transactions = df_source_property_transactions.merge(
            df_get_existing_directions, 
            how="left", 
            left_on="Face",
            right_on="face",
            indicator=False
        )

        df_original_columns_to_use = [
            "Date", "Estimated Value", "Sale Price", "num_rooms", "num_bathrooms", "carpet_area", "property_tax_rate",
            "Face", "city_id", "building_type_id", "property_type_id", "direction_id"
        ]

        df_new_columns_to_use = [
            "transaction_date","property_estimated_value","property_city_id","property_sales_value","building_type_id",
            "property_type_id","bedrooms","bathrooms","property_carpet_area","property_tax_rate","property_face_direction_id"
        ]

        df_source_property_transactions = df_source_property_transactions[df_original_columns_to_use]
        df_source_property_transactions = df_source_property_transactions.rename(columns={
            "Date": "transaction_date",
            "Estimated Value": "property_estimated_value",
            "Sale Price": "property_sales_value",
            "num_rooms": "bedrooms",
            "num_bathrooms": "bathrooms",
            "carpet_area": "property_carpet_area",
            "direction_id": "property_face_direction_id",
            "city_id": "property_city_id"
        })

        df_existing_transactions = self.__get_existing_transactions()[df_new_columns_to_use]

        df_new_property_transactions = df_source_property_transactions.merge(df_existing_transactions, how="outer", on=df_new_columns_to_use, indicator=True)
        df_new_property_transactions = df_new_property_transactions[(df_new_property_transactions["_merge"] == 'left_only')].drop('_merge', axis="columns")
        df_new_property_transactions = df_new_property_transactions[df_new_columns_to_use]

        records_to_append = len(df_new_property_transactions.index)

        if records_to_append > 0:
            self._logger.info(f"TransactionsExtractor.extract There are {records_to_append} records to add to the transactions.")

            try:
                df_new_property_transactions.to_sql('transactions', con=self._get_db_engine(), index=False, if_exists='append')
            except:
                self._notifier.notify()
                return False
            
            self._logger.info("The property transactions were successfully added.")
        else:
            self._logger.info("TransactionsExtractor.extract There are no records to add to the property transactions.")

        return True