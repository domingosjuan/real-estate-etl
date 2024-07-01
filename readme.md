# Real Estate ETL

This is a project that takes data from the following datasets and loads them inside a PostgreSQL relational DB, the mentioned datasets are:

- https://www.kaggle.com/datasets/austinreese/usa-housing-listings/data
- https://www.kaggle.com/datasets/subhiarjaria09/real-estate-property-transactions-dataset

### Used Tools

- Python
- Pandas
- PostgreSQL
- Docker

## Environment Setup

To a better result execute all commands in the project's root folder

### Pre-Req
- Python 3.12 installed and configured
- Latest version of pip installed
- Docker Desktop Installed
- Docker Compose CLI installed

### Configuring Python

Create a Python Virtual Env
> python -m venv .venv

Activate tour virtual env
> source .venv/bin/activate

Install Project Dependencies

> pip install --upgrade -r requirements.txt

## Setting up DB

> docker-compose up -f ./infrastructure/postgresql/docker-compose.yaml

## Setup Source Files
As its a study case project the source files path was set within the project in the folder data present in the project root folder, to setup the files and make them ready to run, you must download them and name them as following:

- https://www.kaggle.com/datasets/austinreese/usa-housing-listings/data -> housing.csv 

And copy it to the folder ```data/usa_housing_listing``` having as the final path ```data/usa_housing_listing/housing.csv```

- https://www.kaggle.com/datasets/subhiarjaria09/real-estate-property-transactions-dataset -> real_estate_transactions.csv

And copy it to the folder ```data/usa_real_state_transactions/real_estate_transactions.csv```

# Running Project

There are two pipelines available which are:

- **housing_listing**: Responsible for ingesting and cleaning the housing data
- **real_estate_transactions**: Responsible for ingesting and cleaning the property transaction data

The application is executed in a CLI kind, where the program main file is called ```app.py``` and is located in the folder ```real_estate_etl```, the execution arguments are all mandatory, and they are the following ones:

- pipeline: The name of the pipeline to be executed
- target_db_host: The target postgresql db host (localhost for this execution example)
- target_db_port: The target postgresql db port (5432 for this execution example)
- target_db_name: The target postgresql db name (real_estate_db for this execution example)
- target_db_username: The target postgresql db username (real_estate_user for this execution example)
- target_db_password: The target postgresql db password (localhost for this execution 1234)

To run the pipeline after configuring your environment, you must use the following commands:

**Housing Pipeline**
> python3 app.py --pipeline housing_listing \\
	 						  - -target_db_host localhost \\
	                          - -target_db_port 5432 \\
	                          - -target_db_name real_estate_db \\
	                          - -target_db_username real_estate_user \\
	                          - -target_db_password 1234

**Transactions Pipeline**
> python3 app.py --pipeline real_estate_transactions \\
	 						  - -target_db_host localhost \\
	                          - -target_db_port 5432 \\
	                          - -target_db_name real_estate_db \\
	                          - -target_db_username real_estate_user \\
	                          - -target_db_password 1234

# Mapped Improvements
There are some mapped improvements to be applied to the code, such as:

- Unify the common extractors based on the target not on the source
- Create an agnostic extractor drive by parameters
- Ingest a public Database of USA cities to enrich our city data in the transaction scope

These changes were not applied to this version due to our MVP perspective and we are looking to attend our ETA securely.