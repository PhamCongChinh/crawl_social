from pydantic_settings import BaseSettings

class Constant(BaseSettings):
    CHUNK_SIZE: int = 5 # default
    CHUNK_SIZE_POST: int = 5
    ATTEMPT: int = 3
    CONCURRENCY: int = 2
    CONCURRENCY_KEYWORD: int = 1

    model_config = {
        "env_file": ".env"
    }