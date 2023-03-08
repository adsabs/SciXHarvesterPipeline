from unittest import TestCase
from harvester.harvester import Harvester_APP
from confluent_kafka.avro import AvroProducer
from mockschemaregistryclient import MockSchemaRegistryClient
import base
from harvester import utils, db
from mock import patch
from mockschemaregistryclient import MockSchemaRegistryClient
import harvester.metadata.arxiv_harvester as arxiv_harvester

class test_harvester(TestCase):
    def test_harvester_task(self):
        mock_job_request = base.mock_job_request()
        with base.base_utils.mock_multiple_targets({ \
            'arxiv_harvesting': patch.object(arxiv_harvester, 'arxiv_harvesting', return_value = "Success"), \
            'update_job_status': patch.object(db, 'update_job_status', return_value = True),\
            'write_status_redis': patch.object(db, 'write_status_redis', return_value = True)
            }) as mocked:
            mock_app = Harvester_APP(proj_home='tests/data')
            mock_app.schema_client = MockSchemaRegistryClient()
            producer = AvroProducer({}, schema_registry=mock_app.schema_client)
            mock_app.harvester_task(mock_job_request, producer)
            self.assertTrue(mocked['arxiv_harvesting'].called)
            self.assertTrue(mocked['update_job_status'].called)
            self.assertTrue(mocked['write_status_redis'].called)