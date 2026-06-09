from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    kafka_topic: str = "movie-events"
    kafka_acks: str = "all"
    kafka_retries: int = 5
    kafka_retry_backoff_ms: int = 300
    generator_enabled: bool = True
    generator_interval_ms: int = 500
    log_level: str = "INFO"

    class Config:
        env_file = ".env"


settings = Settings()
