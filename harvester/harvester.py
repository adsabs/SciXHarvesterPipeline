from datetime import datetime
import time
import logging
import json
import json
import redis
import boto3

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from harvester.metadata.arxiv_harvester import arxiv_harvesting
from harvester.s3_methods import s3_methods

from harvester import db, utils

class Harvester_APP:
    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def _consume_from_topic(self, consumer):
        self.logger.debug("Consuming from Harvester Topic")
        return consumer.poll()
    
    def _init_logger(self):
        logging.basicConfig(level=logging.DEBUG)
        self.logger=logging.getLogger(__name__)
        self.logger.info("Starting Harvester Service Logging")

    def __init__(self, proj_home, start_s3=False):
        self.config = utils.load_config(proj_home)
        self.engine = create_engine(self.config.get('SQLALCHEMY_URL'))
        self.logger = None
        self.schema_client = None
        self._init_logger()
        self.s3Client = s3_methods(boto3.client('s3'))
        self.Session = sessionmaker(self.engine)
        self.redis = redis.StrictRedis(self.config.get('REDIS_HOST', 'localhost'), self.config.get('REDIS_PORT', 6379), charset="utf-8", decode_responses=True) 
    

    def Harvester_task(self, consumer, producer):
        while True:
            msg = self._consume_from_topic(consumer)
            if msg:
                tstamp = datetime.now()
                self.logger.debug("Received message {}".format(msg.value()))
                job_request = msg.value()
                task_args = job_request.get("task_args")
                job_request["status"] = "Processing"
                db.update_job_status(self, job_request["hash"], job_request["status"])
                db.write_status_redis(self.redis, json.dumps({"job_id":job_request["hash"], "status":job_request["status"]}))
                self.logger.debug(b'This message was generated by the Harvester and was read from the gRPC topic %s.' % bytes(str(tstamp), 'utf-8'))
                
                if job_request.get("task") == "ARXIV":
                    if task_args.get('ingest_type') == "metadata":
                        job_request["status"] = arxiv_harvesting(self, job_request, producer)
                else:
                    job_request['status'] = "Error"
                db.update_job_status(self, job_request["hash"], status = job_request["status"])
                db.write_status_redis(self.redis, json.dumps({"job_id":job_request["hash"], "status":job_request["status"]}))
                tstamp = datetime.now()            
                self.logger.info(b'Done %s.' % bytes(str(tstamp), 'utf-8'))

            else:
                self.logger.debug("No new messages")
                time.sleep(2)
                continue
