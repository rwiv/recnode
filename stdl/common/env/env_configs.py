from pydantic import BaseModel, Field


class AmqpConfig(BaseModel):
    host: str = Field(min_length=1)
    port: int
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)
