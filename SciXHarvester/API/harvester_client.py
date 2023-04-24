# Copyright 2021 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python AsyncIO implementation of the GRPC harvester.HarvesterInit client."""

import argparse
import asyncio
import json
import logging

import grpc
import grpc_modules.harvester_grpc as harvester_grpc
from avro_serializer import AvroSerialHelper
from confluent_kafka.schema_registry import SchemaRegistryClient


def get_schema(app, schema_client, schema_name):
    try:
        avro_schema = schema_client.get_latest_version(schema_name)
        app.logger.info("Found schema: {}".format(avro_schema.schema.schema_str))
    except Exception as e:
        avro_schema = None
        app.logger.warning("Could not retrieve avro schema with exception: {}".format(e))

    return avro_schema.schema.schema_str


schema_client = SchemaRegistryClient({"url": "http://localhost:8081"})


class Logging:
    def __init__(self, logger):
        self.logger = logger


logger = Logging(logging)
schema = get_schema(logger, schema_client, "HarvesterInputSchema")

avroserialhelper = AvroSerialHelper(schema)


async def run() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help="commands", dest="action")
    process_parser = subparsers.add_parser(
        "HARVESTER_INIT", help="Initialize a job with given inputs"
    )
    process_parser.add_argument(
        "--task_args",
        action="store",
        dest="job_args",
        type=str,
        help="JSON dump containing arguments for Harvester Processes",
    )
    process_parser.add_argument(
        "--persistence",
        action="store_true",
        dest="persistence",
        default=False,
        help="Specify whether server keeps channel open to client during processing.",
    )
    process_parser.add_argument(
        "--task",
        action="store",
        dest="task",
        type=str,
        help="Specify whether server keeps channel open to client during processing.",
    )

    process_parser = subparsers.add_parser(
        "HARVESTER_MONITOR", help="Initialize a job with given inputs"
    )
    process_parser.add_argument(
        "--job_id", action="store", dest="job_id", type=str, help="Job ID string to query."
    )
    process_parser.add_argument(
        "--persistence",
        action="store_true",
        dest="persistence",
        default=False,
        help="Specify whether server keeps channel open to client during processing.",
    )
    args = parser.parse_args()

    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        s = {}
        # Read from an async generator
        if args.action == "HARVESTER_INIT":
            if args.job_args:
                task_args = json.loads(args.job_args)
                s["task_args"] = task_args
                if task_args.get("ingest"):
                    s["task_args"]["ingest"] = bool(task_args["ingest"])
            s["task_args"]["persistence"] = args.persistence
            s["task"] = args.task
            try:
                stub = harvester_grpc.HarvesterInitStub(channel, avroserialhelper)
                async for response in stub.initHarvester(s):
                    print(response)

            except grpc.aio._call.AioRpcError as e:
                code = e.code()
                print(
                    "gRPC server connection failed with status {}: {}".format(
                        code.name, code.value
                    )
                )

        elif args.action == "HARVESTER_MONITOR":
            s["task"] = "MONITOR"
            s["task_args"] = {"persistence": args.persistence}
            s["hash"] = args.job_id

            stub = harvester_grpc.HarvesterMonitorStub(channel, avroserialhelper)
            async for response in stub.monitorHarvester(s):
                print(response)


if __name__ == "__main__":
    logging.basicConfig()
    asyncio.run(run())
