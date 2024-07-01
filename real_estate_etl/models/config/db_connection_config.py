from dataclasses import dataclass

@dataclass
class DBConnectionConfig:

    db_host: str
    db_port: str
    db_name: str
    db_username: str
    db_password: str
