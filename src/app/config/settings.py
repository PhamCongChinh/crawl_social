from pydantic_settings  import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "FastAPI App Celery"
    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DB: str = "mydb"
    SCRAPFLY_API_KEY: str = "your-api-key"

    PYTHONPATH: str = "/app/src"
    POETRY_VIRTUALENVS_IN_PROJECT: bool = True

    REDIS_HOST: str = "redis_server"
    REDIS_PORT: int = 6379
    REDIS_BROKER_DB: int = 0
    REDIS_BACKEND_DB: int = 1

    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "your_password"
    POSTGRES_DB: str = "your_db"
    POSTGRES_SCHEMA: str = "public"

    @property
    def redis_broker_url(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_BROKER_DB}"

    @property
    def redis_backend_url(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_BACKEND_DB}"
    
    @property
    def postgres_url(self):
        return (
            f"postgresql://{self.POSTGRES_USER}:"
            f"{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:"
            f"{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
            f"?options=-csearch_path%3D{self.POSTGRES_SCHEMA}"
        )

    model_config = {
        "env_file": ".env"
    }

settings = Settings()
print("✅ Đã khởi tạo Settings xong")