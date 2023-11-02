from unittest import TestCase

import base
from confluent_kafka.avro import AvroProducer
from mock import patch
from SciXPipelineUtils.scix_uuid import scix_uuid as uuid

import harvester.metadata.arxiv_harvester as arxiv_harvester
from harvester import db
from harvester.harvester import Harvester_APP
from tests.common.mockschemaregistryclient import MockSchemaRegistryClient


class test_harvester(TestCase):
    def test_harvester_task(self):
        mock_job_request = base.mock_job_request()
        with base.base_utils.mock_multiple_targets(
            {
                "arxiv_harvesting": patch.object(
                    arxiv_harvester, "arxiv_harvesting", return_value="Success"
                ),
                "update_job_status": patch.object(db, "update_job_status", return_value=True),
                "write_status_redis": patch.object(db, "write_status_redis", return_value=True),
            }
        ) as mocked:
            mock_app = Harvester_APP(proj_home="SciXHarvester/tests/stubdata/")
            mock_app.schema_client = MockSchemaRegistryClient()
            producer = AvroProducer({}, schema_registry=mock_app.schema_client)
            mock_app.harvester_task(mock_job_request, producer)
            self.assertTrue(mocked["arxiv_harvesting"].called)
            self.assertTrue(mocked["update_job_status"].called)
            self.assertTrue(mocked["write_status_redis"].called)

    def test_writing_harvester_output(self):
        mock_app = Harvester_APP(proj_home="SciXHarvester/tests/stubdata/")
        record_id = uuid.uuid4()
        date = "2023-04-28 17:48:29.354791"
        s3_key = "/20230428/7eceaca5-9b62-4e10-a153-a882b209df9f"
        checksum = "947e77d2c4b4ec4ffb55a089e92bc538"
        source = "ARXIV"
        db.write_harvester_record(mock_app, record_id, date, s3_key, checksum, source)
        mock_app = Harvester_APP(proj_home="SciXHarvester/tests/stubdata/")
        with mock_app.session_scope() as session:
            output_id = db.get_harvester_record(session, record_id).id
        self.assertEqual(output_id, record_id)
