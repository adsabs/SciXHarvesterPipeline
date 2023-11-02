import enum

from SciXPipelineUtils.scix_uuid import scix_uuid as uuid
from sqlalchemy import Column, DateTime, Enum, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Status(enum.Enum):
    Pending = 1
    Processing = 2
    Error = 3
    Success = 4


class Source(enum.Enum):
    ARXIV = 1
    APS = 2
    AAS = 3
    MNRAS = 4
    PNAAS = 5


class gRPC_status(Base):
    """
    gRPC table
    table containing the given status of every job passed through the gRPC API
    """

    __tablename__ = "grpc_status"
    id = Column(Integer, primary_key=True)
    job_hash = Column(String, unique=True)
    job_request = Column(String)
    status = Column(Enum(Status))
    timestamp = Column(DateTime)


class Harvester_record(Base):
    """
    ArXiV records table
    table containing the relevant information for harvested arxiv records.
    """

    __tablename__ = "harvester_records"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    s3_key = Column(String)
    date = Column(DateTime)
    checksum = Column(String)
    source = Column(Enum(Source))
