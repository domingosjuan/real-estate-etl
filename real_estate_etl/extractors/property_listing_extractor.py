import pandas as pd

from utils.log.custom_logger import CustomLogger
from models.config.db_connection_config import DBConnectionConfig
from extractors.abstractions.abstract_extractor import AbstractExtractor


class PropertyListingExtractor(AbstractExtractor):

    def __init__(
        self, 
        source_df: pd.DataFrame,
        target_db_config: DBConnectionConfig,
        logger: CustomLogger
    ) -> None:
        
        super().__init__(target_db_config=target_db_config)
        
        self._logger = logger
        self._source_df = source_df

    def __get_existing_property_listing(self) -> pd.DataFrame:
        """
            A private method that returns the existing listed properties

            returns
                pd.DataFrame: A pandas DataFrame containing the existing listed properties
        """

        return pd.read_sql(sql="SELECT * FROM property_listing", con=self._get_db_engine())
    

    def __get_existing_property_types(self) -> pd.DataFrame:
        """
            A private method that returns the existing property types

            returns
                pd.DataFrame: A pandas DataFrame containing the existing property types
        """

        return pd.read_sql(sql="SELECT property_type_id, LOWER(description) AS type FROM property_types", con=self._get_db_engine())
    

    def __get_existing_locations(self) -> pd.DataFrame:
        """
            A private method that returns the existing locations

            returns
                pd.DataFrame: A pandas DataFrame containing the existing locations
        """

        query = f"""
            SELECT locations.location_id      AS location_id,
                   LOWER(regions.description) AS region,
                   regions.region_url         AS region_url,
                   LOWER(states.acronym)      AS state
              FROM locations
         LEFT JOIN regions
                ON locations.region_id = regions.region_id
         LEFT JOIN states
                ON states.state_id = locations.state_id
        """

        return pd.read_sql(sql=query, con=self._get_db_engine())
    

    def __get_existing_parking_options(self) -> pd.DataFrame:
        """
            A private method that returns the existing parking options in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing parking options
        """

        return pd.read_sql(sql="SELECT parking_option_id, LOWER(description) AS parking_options FROM parking_options", con=self._get_db_engine())
    

    def __get_existing_laundry_options(self) -> pd.DataFrame:
        """
            A private method that returns the existing laundry options in the target db

            returns
                pd.DataFrame: A pandas DataFrame containing the existing laundry options
        """

        return pd.read_sql(sql="SELECT laundry_option_id, LOWER(description) AS laundry_options FROM laundry_options", con=self._get_db_engine())
    

    def extract(self):
        """
            A method that extracts the the property listing from a housing_listing file

            returns:
                bool: A boolean flag indicating if the extraction was successfully executed
        """

        self._logger.info("PropertyListingExtractor.extract Starting Property Listing Extraction")

        df_source_property_listing = (
            self._source_df
                .drop_duplicates()
                .dropna()
        )

        df_source_property_listing["region"] = df_source_property_listing["region"].str.lower()
        df_source_property_listing["region_url"] = df_source_property_listing["region_url"].str.lower()
        df_source_property_listing["state"] = df_source_property_listing["state"].str.lower()
        df_source_property_listing["type"] = df_source_property_listing["type"].str.lower()
        df_source_property_listing["parking_options"] = df_source_property_listing["parking_options"].str.lower()

        df_source_property_listing = df_source_property_listing.astype(
            dict.fromkeys(["cats_allowed", "dogs_allowed", "smoking_allowed", "wheelchair_access", "electric_vehicle_charge", "comes_furnished"], bool)
        )

        df_existing_locations = self.__get_existing_locations()
        
        df_source_property_listing = df_source_property_listing.merge(
            df_existing_locations, 
            how="left", 
            left_on=["region", "region_url", "state"], 
            right_on=["region", "region_url", "state"], 
            indicator=False
        )

        df_existing_property_types = self.__get_existing_property_types()

        df_source_property_listing = df_source_property_listing.merge(
            df_existing_property_types, 
            how="left", 
            on="type", 
            indicator=False
        )

        df_existing_parking_options = self.__get_existing_parking_options()

        df_source_property_listing = df_source_property_listing.merge(
            df_existing_parking_options, 
            how="left", 
            on="parking_options", 
            indicator=False
        )

        df_existing_laundry_options = self.__get_existing_laundry_options()

        df_source_property_listing = df_source_property_listing.merge(
            df_existing_laundry_options, 
            how="left", 
            on="laundry_options", 
            indicator=False
        )

        df_columns_to_use = [
            "id", "url", "location_id", "price", "property_type_id", "sqfeet", "beds", "baths",
            "cats_allowed", "dogs_allowed", "smoking_allowed", "wheelchair_access", "electric_vehicle_charge",
            "comes_furnished", "laundry_option_id", "parking_option_id", "image_url", "description", "lat", "long"
        ]

        df_source_property_listing = df_source_property_listing[df_columns_to_use]

        df_existing_property_listing = self.__get_existing_property_listing()[["property_listing_id"]]

        df_new_property_listing = df_source_property_listing.merge(df_existing_property_listing, how="outer", left_on="id", right_on="property_listing_id", indicator=True)
        
        df_new_property_listing = df_new_property_listing[(df_new_property_listing["_merge"] == 'left_only')].drop('_merge', axis="columns")
        df_new_property_listing = df_new_property_listing[df_columns_to_use]

        df_new_property_listing = df_new_property_listing.rename(columns={
            "id": "property_listing_id",
            "url": "property_listing_url",
            "image_url": "property_image_url",
            "description": "property_description",
            "location_id": "property_location_id",
            "long": "property_location_longitude",
            "lat": "property_location_latitude",
            "sqfeet": "property_square_feet",
            "price": "property_price",
            "beds": "bedrooms",
            "baths": "bathrooms"
        })

        records_to_append = len(df_new_property_listing.index)

        if records_to_append > 0:
            self._logger.info(f"PropertyListingExtractor.extract There are {records_to_append} records to add to the property listing.")

            # try:
            df_new_property_listing.to_sql('property_listing', con=self._get_db_engine(), index=False, if_exists='append')
            # except:
            #     self._notifier.notify()
            #     return False
            
            self._logger.info("The property listings were successfully added.")
        else:
            self._logger.info("PropertyListingExtractor.extract There are no records to add to the property listings.")

        return True
