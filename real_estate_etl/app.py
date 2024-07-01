import sys
import argparse

import pandas as pd

from extractors.abstractions.abstract_extractor import AbstractExtractor
from extractors.laundry_options_extractor import LaundryOptionsExtractor
from extractors.parking_options_extractor import ParkingOptionsExtractor
from extractors.regions_extractor import RegionsExtractor
from extractors.locations_extractor import LocationsExtractor
from extractors.property_type_extractors import PropertyTypeExtractor
from extractors.property_listing_extractor import PropertyListingExtractor

from models.config.db_connection_config import DBConnectionConfig

from utils.log.custom_logger import CustomLogger

pipelines = {
    "housing_listing": {
        "input_file": "../data/usa_housing_listing/housing.csv",
        "steps": {
            1: LaundryOptionsExtractor,
            2: ParkingOptionsExtractor,
            3: RegionsExtractor,
            4: LocationsExtractor,
            5: PropertyTypeExtractor,
            6: PropertyListingExtractor
        }
    }
}

if __name__ == "__main__":

    logger = CustomLogger()

    logger.info("Initializing Real Estate Data Loader")

    argument_parser = argparse.ArgumentParser(
        description="Ingest Real Estate Data for the Pre-Defined Pipelines ['housing_listing']",
        prefix_chars="-",
        allow_abbrev=False,
        add_help=True
    )

    argument_parser.add_argument("--pipeline", type=str, help="The target pipeline name within the allowed pipelines ['house_listing']")
    argument_parser.add_argument("--target_db_host", type=str, help="The target database host")
    argument_parser.add_argument("--target_db_port", type=str, help="The target database port")
    argument_parser.add_argument("--target_db_name", type=str, help="The target database name")
    argument_parser.add_argument("--target_db_username", type=str, help="The target database username")
    argument_parser.add_argument("--target_db_password", type=str, help="The target database password")

    args = argument_parser.parse_args()

    pipeline = args.pipeline

    logger.info(f"Running the following Target pipeline: {pipeline}")

    if pipeline not in pipelines.keys():
        raise ValueError(f"The {pipeline} pipeline is not valid, please chose of these: [{','.join(pipelines.keys())}]")

    pipeline = pipelines[pipeline]

    df_source_data = pd.read_csv(filepath_or_buffer=pipeline["input_file"], sep=",")

    db_config = DBConnectionConfig(
        db_host=args.target_db_host,
        db_port=args.target_db_port,
        db_name=args.target_db_name,
        db_username=args.target_db_username,
        db_password=args.target_db_password
    )   


    pipeline_steps = list(pipeline["steps"].keys())
    pipeline_steps.sort()

    for step in pipeline_steps:
        success = pipeline["steps"][step](source_df=df_source_data,target_db_config=db_config, logger=logger).extract()

        if not success:
            raise Exception("The is a processing step failed.")
