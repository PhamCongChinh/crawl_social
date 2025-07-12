from pydantic_settings import BaseSettings

class Constant(BaseSettings):
    CHUNK_SIZE: int = 5 # default
    ATTEMPT: int = 3
    CONCURRENCY: int = 2

    model_config = {
        "env_file": ".env"
    }