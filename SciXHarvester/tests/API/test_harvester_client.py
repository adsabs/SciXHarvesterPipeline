import logging
from unittest import TestCase

import pytest
from confluent_kafka.schema_registry import Schema

from API.harvester_client import Logging, get_schema, input_parser, output_message
from tests.common.mockschemaregistryclient import MockSchemaRegistryClient


class TestHarvesterClient(TestCase):
    def test_get_schema(self):
        logger = Logging(logging)
        schema_client = MockSchemaRegistryClient()
        VALUE_SCHEMA_FILE = "tests/stubdata/AVRO_schemas/HarvesterInputSchema.avsc"
        VALUE_SCHEMA_NAME = "HarvesterInputSchema"
        value_schema = open(VALUE_SCHEMA_FILE).read()

        schema_client.register(VALUE_SCHEMA_NAME, Schema(value_schema, "AVRO"))
        schema = get_schema(logger, schema_client, VALUE_SCHEMA_NAME)
        self.assertEqual(value_schema, schema)

    def test_get_schema_failure(self):
        logger = Logging(logging)
        schema_client = MockSchemaRegistryClient()
        with pytest.raises(Exception):
            get_schema(logger, schema_client, "FakeSchema")

    def test_input_parser(self):
        input_args = [
            "HARVESTER_MONITOR",
            "--job_id",
            "'c98b5b0f5e4dce3197a4a9a26d124d036f293a9a90a18361f475e4f08c19f2da'",
        ]
        args = input_parser(input_args)
        self.assertEqual(args.action, input_args[0])
        self.assertEqual(args.job_id, input_args[2])

        s = output_message(args)
        self.assertEqual(s["task"], "MONITOR")

        input_args = [
            "HARVESTER_INIT",
            "--task",
            "ARXIV",
            "--task_args",
            '{"ingest": "True", "ingest_type": "metadata", "daterange":"2023-04-26"}',
            "--persistence",
        ]
        args = input_parser(input_args)
        self.assertEqual(args.action, input_args[0])
        self.assertEqual(args.task, input_args[2])
        self.assertEqual(args.job_args, input_args[4])
        self.assertEqual(args.persistence, True)

        s = output_message(args)
        print(s)
        self.assertEqual(s["task"], "ARXIV")
