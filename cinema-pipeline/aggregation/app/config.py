from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 9000
    clickhouse_db: str = "cinema"
    clickhouse_user: str = "cinema_user"
    clickhouse_password: str = "cinema_pass"

    postgres_dsn: str = "postgresql://cinema_user:cinema_pass@localhost:5432/cinema_aggregates"

    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "movie-analytics"

    aggregation_schedule: str = "0 * * * *"

    log_level: str = "INFO"

    class Config:
        env_file = ".env"


settings = Settings()
