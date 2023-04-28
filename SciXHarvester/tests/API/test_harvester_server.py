import logging
from concurrent import futures
from unittest import TestCase

import grpc
import pytest
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import Schema

from API.avro_serializer import AvroSerialHelper
from API.grpc_modules import harvester_grpc
from API.harvester_client import Logging, get_schema
from API.harvester_server import Harvester
from tests.common.mockschemaregistryclient import MockSchemaRegistryClient


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

        self.producer = AvroProducer({}, schema_registry=self.schema_client)

        harvester_grpc.add_HarvesterInitServicer_to_server(
            Harvester(self.producer, self.schema, self.schema_client),
            self.server,
            self.avroserialhelper,
        )
        self.port = 55551
        self.server.add_insecure_port(f"[::]:{self.port}")
        self.server.start()

    def tearDown(self):
        self.server.stop(None)

    def test_Harvester_server(self):
        s = {}

        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            stub = harvester_grpc.HarvesterInitStub(channel, self.avroserialhelper)
            with pytest.raises(SystemExit):
                stub.initHarvester(s)

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
            stub = harvester_grpc.HarvesterInitStub(channel, self.avroserialhelper)
            responses = stub.initHarvester(s)
            for response in list(responses):
                print(response)

    # def test_harvester_initHarvester(self):
    #     harvester_init_class=Harvester(self.producer, self.schema, self.schema_client)
    #     s={'task_args': {'ingest': True, 'ingest_type': 'metadata', 'daterange': '2023-04-26', 'persistence': True}, 'task': 'ARXIV'}
    #     import pudb
    #     pudb.set_trace()
    #     harvester_init_class.initHarvester(s, grpc.aio.ServicerContext)
