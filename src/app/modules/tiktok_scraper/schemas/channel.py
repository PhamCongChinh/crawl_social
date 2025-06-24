from pydantic import BaseModel


class ChannelRequest(BaseModel):
    url: str
    org_id: int