import logging
from concurrent import futures
from unittest import TestCase

import grpc
import pytest
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import Schema
from mock import patch
from SciXPipelineUtils.avro_serializer import AvroSerialHelper

from API.grpc_modules import harvester_grpc
from API.harvester_client import get_schema
from API.harvester_server import Harvester, Listener, Logging
from harvester import db
from tests.API import base
from tests.common.mockschemaregistryclient import MockSchemaRegistryClient


class fake_db_entry(object):
    def __init__(self, status="Success"):
        self.name = status


class HarvesterServer(TestCase):
    def setUp(self):
        """Instantiate a harvester server and return a stub for use in tests"""
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        self.logger = Logging(logging)
        self.schema_client = MockSchemaRegistryClient()
        self.VALUE_SCHEMA_FILE = (
            "SciXHarvester/tests/stubdata/AVRO_schemas/HarvesterInputSchema.avsc"
        )
        self.VALUE_SCHEMA_NAME = "HarvesterInputSchema"
        self.value_schema = open(self.VALUE_SCHEMA_FILE).read()

        self.schema_client.register(self.VALUE_SCHEMA_NAME, Schema(self.value_schema, "AVRO"))
        self.schema = get_schema(self.logger, self.schema_client, self.VALUE_SCHEMA_NAME)
        self.avroserialhelper = AvroSerialHelper(self.schema, self.logger.logger)

        OUTPUT_VALUE_SCHEMA_FILE = (
            "SciXHarvester/tests/stubdata/AVRO_schemas/HarvesterOutputSchema.avsc"
        )
        OUTPUT_VALUE_SCHEMA_NAME = "HarvesterOutputSchema"
        output_value_schema = open(OUTPUT_VALUE_SCHEMA_FILE).read()

        self.schema_client.register(OUTPUT_VALUE_SCHEMA_NAME, Schema(output_value_schema, "AVRO"))
        self.producer = AvroProducer({}, schema_registry=MockSchemaRegistryClient())

        harvester_grpc.add_HarvesterInitServicer_to_server(
            Harvester(self.producer, self.schema, self.schema_client, self.logger.logger),
            self.server,
            self.avroserialhelper,
        )

        harvester_grpc.add_HarvesterMonitorServicer_to_server(
            Harvester(self.producer, self.schema, self.schema_client, self.logger.logger),
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
            with pytest.raises(grpc.RpcError):
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
            stub = harvester_grpc.HarvesterInitStub(channel, self.avroserialhelper)
            responses = stub.initHarvester(s)
            for response in list(responses):
                self.assertEqual(response.get("status"), "Pending")
                self.assertNotEqual(response.get("hash"), None)

    def test_Harvester_server_init_persistence(self):
        s = {
            "task_args": {
                "ingest": True,
                "ingest_type": "metadata",
                "daterange": "2023-04-26",
                "persistence": True,
            },
            "task": "ARXIV",
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Processing")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Success"])
                    ),
                }
            ):
                stub = harvester_grpc.HarvesterInitStub(channel, self.avroserialhelper)
                responses = stub.initHarvester(s)
                final_response = []
                for response in list(responses):
                    self.assertNotEqual(response.get("hash"), None)
                    final_response.append(response.get("status"))
                self.assertEqual(final_response, ["Pending", "Processing", "Success"])

    def test_Harvester_server_init_persistence_error_redis(self):
        s = {
            "task_args": {
                "ingest": True,
                "ingest_type": "metadata",
                "daterange": "2023-04-26",
                "persistence": True,
            },
            "task": "ARXIV",
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Processing")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Error"])
                    ),
                }
            ):
                stub = harvester_grpc.HarvesterInitStub(channel, self.avroserialhelper)
                responses = stub.initHarvester(s)
                final_response = []
                for response in list(responses):
                    self.assertNotEqual(response.get("hash"), None)
                    final_response.append(response.get("status"))
                self.assertEqual(final_response, ["Pending", "Processing", "Error"])

    def test_Harvester_server_init_persistence_error_db(self):
        s = {
            "task_args": {
                "ingest": True,
                "ingest_type": "metadata",
                "daterange": "2023-04-26",
                "persistence": True,
            },
            "task": "ARXIV",
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Error")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Error"])
                    ),
                }
            ):
                stub = harvester_grpc.HarvesterInitStub(channel, self.avroserialhelper)
                responses = stub.initHarvester(s)
                final_response = []
                for response in list(responses):
                    self.assertNotEqual(response.get("hash"), None)
                    final_response.append(response.get("status"))
                self.assertEqual(final_response, ["Pending", "Error"])

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

    def test_Harvester_server_monitor_persistent_success(self):
        s = {
            "task": "MONITOR",
            "hash": "c98b5b0f5e4dce3197a4a9a26d124d036f293a9a90a18361f475e4f08c19f2da",
            "task_args": {"persistence": True},
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Success")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Success"])
                    ),
                }
            ):
                stub = harvester_grpc.HarvesterMonitorStub(channel, self.avroserialhelper)
                responses = stub.monitorHarvester(s)
                for response in list(responses):
                    self.assertEqual(response.get("status"), "Success")
                    self.assertEqual(response.get("hash"), s.get("hash"))

    def test_Harvester_server_monitor_persistent_error_db(self):
        s = {
            "task": "MONITOR",
            "hash": "c98b5b0f5e4dce3197a4a9a26d124d036f293a9a90a18361f475e4f08c19f2da",
            "task_args": {"persistence": True},
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Error")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Success"])
                    ),
                }
            ):
                stub = harvester_grpc.HarvesterMonitorStub(channel, self.avroserialhelper)
                responses = stub.monitorHarvester(s)
                for response in list(responses):
                    self.assertEqual(response.get("status"), "Error")
                    self.assertEqual(response.get("hash"), s.get("hash"))

    def test_Harvester_server_monitor_persistent_error_redis(self):
        s = {
            "task": "MONITOR",
            "hash": "c98b5b0f5e4dce3197a4a9a26d124d036f293a9a90a18361f475e4f08c19f2da",
            "task_args": {"persistence": True},
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Processing")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Error"])
                    ),
                }
            ):
                stub = harvester_grpc.HarvesterMonitorStub(channel, self.avroserialhelper)
                responses = stub.monitorHarvester(s)
                final_responses = []
                for response in list(responses):
                    final_responses.append(response.get("status"))
                    self.assertEqual(response.get("hash"), s.get("hash"))
                self.assertEqual(final_responses, ["Processing", "Error"])

    def test_Harvester_server_monitor_no_hash(self):
        s = {
            "task": "MONITOR",
            "hash": None,
            "task_args": {"persistence": True},
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Error")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Success"])
                    ),
                }
            ):
                stub = harvester_grpc.HarvesterMonitorStub(channel, self.avroserialhelper)
                responses = stub.monitorHarvester(s)
                for response in list(responses):
                    self.assertEqual(response.get("status"), "Error")
                    self.assertEqual(response.get("hash"), s.get("hash"))

    def test_Harvester_server_init_and_monitor(self):
        cls = Harvester(self.producer, self.schema, self.schema_client, self.logger.logger)
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
            stub = harvester_grpc.HarvesterInitStub(channel, self.avroserialhelper)
            responses = stub.initHarvester(s)
            output_hash = None
            for response in list(responses):
                output_hash = response.get("hash")
                self.assertEqual(response.get("status"), "Pending")
                self.assertNotEqual(response.get("hash"), None)

                s = {
                    "task": "MONITOR",
                    "hash": output_hash,
                    "task_args": {"persistence": False},
                }

        # Test update_job_status as well to mimic the Pipeline updating the status.
        db.update_job_status(cls, output_hash, status="Processing")

        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            stub = harvester_grpc.HarvesterMonitorStub(channel, self.avroserialhelper)
            responses = stub.monitorHarvester(s)
            for response in list(responses):
                self.assertEqual(response.get("status"), "Processing")
                self.assertEqual(response.get("hash"), s.get("hash"))
