import logging
from concurrent import futures
from unittest import TestCase

import grpc
import pytest
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import Schema
from mock import patch

from API.avro_serializer import AvroSerialHelper
from API.grpc_modules import harvester_grpc
from API.harvester_client import Logging, get_schema
from API.harvester_server import Harvester
from harvester import db
from tests.API import base
from tests.common.mockschemaregistryclient import MockSchemaRegistryClient


class fake_db_entry(object):
    def __init__(self):
        self.name = "Success"


class HarvesterServer(TestCase):
    def setUp(self):
        """Instantiate a harvester server and return a stub for use in tests"""
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        self.logger = Logging(logging)
        self.schema_client = MockSchemaRegistryClient()
        self.VALUE_SCHEMA_FILE = "tests/stubdata/AVRO_schemas/HarvesterInputSchema.avsc"
        self.VALUE_SCHEMA_NAME = "HarvesterInputSchema"
        self.value_schema = open(self.VALUE_SCHEMA_FILE).read()

        self.schema_client.register(self.VALUE_SCHEMA_NAME, Schema(self.value_schema, "AVRO"))
        self.schema = get_schema(self.logger, self.schema_client, self.VALUE_SCHEMA_NAME)
        self.avroserialhelper = AvroSerialHelper(self.schema, self.logger.logger)

        OUTPUT_VALUE_SCHEMA_FILE = "tests/stubdata/AVRO_schemas/HarvesterOutputSchema.avsc"
        OUTPUT_VALUE_SCHEMA_NAME = "HarvesterOutputSchema"
        output_value_schema = open(OUTPUT_VALUE_SCHEMA_FILE).read()

        self.schema_client.register(OUTPUT_VALUE_SCHEMA_NAME, Schema(output_value_schema, "AVRO"))
        self.producer = AvroProducer({}, schema_registry=MockSchemaRegistryClient())

        harvester_grpc.add_HarvesterInitServicer_to_server(
            Harvester(self.producer, self.schema, self.schema_client),
            self.server,
            self.avroserialhelper,
        )

        harvester_grpc.add_HarvesterMonitorServicer_to_server(
            Harvester(self.producer, self.schema, self.schema_client),
            self.server,
            self.avroserialhelper,
        )
        self.port = 55551
        self.server.add_insecure_port(f"[::]:{self.port}")
        self.server.start()

    def tearDown(self):
        self.server.stop(None)

    def test_Harvester_server_bad_entry(self):
        s = {}

        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            stub = harvester_grpc.HarvesterInitStub(channel, self.avroserialhelper)
            with pytest.raises(SystemExit):
                stub.initHarvester(s)

    def test_Harvester_server_init(self):
        s = {
            "task_args": {
                "ingest": True,
                "ingest_type": "metadata",
                "daterange": "2023-04-26",
                "persistence": False,
            },
            "task": "ARXIV",
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {"write_job_status": patch.object(db, "write_job_status", return_value=True)}
            ):
                stub = harvester_grpc.HarvesterInitStub(channel, self.avroserialhelper)
                responses = stub.initHarvester(s)
                for response in list(responses):
                    self.assertEqual(response.get("status"), "Pending")
                    self.assertNotEqual(response.get("hash"), None)

    def test_Harvester_server_monitor(self):
        s = {
            "task": "MONITOR",
            "hash": "c98b5b0f5e4dce3197a4a9a26d124d036f293a9a90a18361f475e4f08c19f2da",
            "task_args": {"persistence": False},
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry()
                    )
                }
            ):
                stub = harvester_grpc.HarvesterMonitorStub(channel, self.avroserialhelper)
                responses = stub.monitorHarvester(s)
                for response in list(responses):
                    self.assertEqual(response.get("status"), "Success")
                    self.assertEqual(response.get("hash"), s.get("hash"))
