from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Source(Base):
    __tablename__ = "dim_sources"

    source_id = Column(Integer, primary_key=True)
    source_id_code = Column(String)
    source_name = Column(String)
    source_type = Column(Integer)
    source_url = Column(String)
    source_channel = Column(String)