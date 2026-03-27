from pydantic import BaseSettings


class MysqlConfig(BaseSettings):
    MYSQL_URI: str = ""

    class Config:
        env_file = ".env"


mysql_config = MysqlConfig()
