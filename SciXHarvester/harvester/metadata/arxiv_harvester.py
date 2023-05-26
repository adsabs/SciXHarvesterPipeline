import logging as logger
import re
import time
import uuid
from datetime import datetime

import requests
from adsingestp.parsers import arxiv
from SciXPipelineUtils import utils

from harvester import db
from harvester.base.OAIHarvester import OAIHarvester as OAI

MAX_RETRIES = 5


def arxiv_harvesting(app, job_request, producer):
    """
    Main harvesting routine for arxiv metadata.

    job_request: (json) task message passed to Harvester input topic.
    producer: The harvester kafka producer instance

    return: (str) The final state of the harvesting process.
    """
    datestamp = datetime.now().strftime("%Y%m%d")
    resumptionToken = job_request["task_args"].get("resumptionToken")
    daterange = job_request["task_args"].get("daterange")
    app.logger.info("{}, {}, {}".format(daterange, resumptionToken, datestamp))
    harvester_output_schema = utils.get_schema(
        app, app.schema_client, app.config.get("HARVESTER_OUTPUT_SCHEMA")
    )

    harvester = ArXiV_Harvester(
        app.config.get("ARXIV_OAI_URL"), daterange=daterange, resumptionToken=resumptionToken
    )

    for record in harvester:
        # Assign ID to new record
        record_id = uuid.uuid4()
        # Generate filepath for S3
        file_path = "/{}/{}".format(datestamp, record_id)
        # write record to S3
        checksum = None
        for provider in app.s3Clients.keys():
            try:
                checksum = app.s3Clients[provider].write_object_s3(
                    file_bytes=bytes(record, "utf-8"), object_name=file_path
                )
            except Exception as e:
                app.logger.error(
                    "Failed to write to S3 provider: {} with Exception: {}".format(provider, e)
                )

        if checksum:
            app.logger.debug("AWS checksum for {} is: {}".format(record_id, checksum))
            s3_key = file_path
            produce = db.write_harvester_record(
                app, record_id, datetime.now(), s3_key, str(checksum), job_request.get("task")
            )
            if produce:
                producer_message = {
                    "record_id": str(record_id),
                    "record_xml": record,
                    "task": job_request.get("task"),
                    "datetime": datetime.now(),
                }
                producer.produce(
                    topic=app.config.get("HARVESTER_OUTPUT_TOPIC"),
                    value=producer_message,
                    value_schema=harvester_output_schema,
                )
        else:
            app.logger.error("No checksums generated, All S3 uploads must have failed. Stopping.")
            return "Error"

    return "Success"


class ArXiV_Harvester(OAI):
    def __init__(self, harvest_url, daterange, resumptionToken):
        """
        Initialization of harvesting class.

        url: (str) The OAI harvesting url
        params: (json) The required requests parameters
        daterange: (str) A specified harvesting date range.
        parsed_records: (array) Calls harvest method to produce an array of records accessible from the __next__ method.
        """
        self.url = harvest_url
        self.params = {"metadataPrefix": "oai_dc"}
        self.daterange = daterange
        self.raw_xml = None
        self.fullHarvest = False
        self.parsed_records = self.harvest_arxiv(resumptionToken)

    def harvest_arxiv(self, resumptionToken=None):
        """
        daterange: (str) date with value given as YYYY-MM-DD
        resumptionToken: (str) value returned by previous API call for paging.

        return: (json) ArXiV API response
        """

        success = False
        retries = 0

        try:
            while success is not True:
                """
                This loop:
                1. Sends the relevant request to the ArXiV API
                2. Checks to make sure we aren't receiving any flow control responses.
                3. If we are it waits the specified amount of time before proceeding.
                4. If we repeatedly hit 503 or any other error, we stop.
                """
                if not resumptionToken:
                    self.params["from"] = self.daterange

                else:
                    # specifying any other query params besides the verb with the resumptionToken will result in an error.
                    self.params = {"resumptionToken": resumptionToken}

                try:
                    raw_response = self.ListRecords(self.url, self.params)
                    self.raw_xml = raw_response.text
                    """
                    Still need to write the code that extracts the retry-after time from the response.
                    """
                    if not raw_response.ok:
                        if raw_response.status_code == 503 and retries < MAX_RETRIES:
                            logger.info(raw_response.text)
                            retries += 1
                            search = re.compile(r"\<h1\>Retry after ([0-9]+) seconds\</h1\>")
                            sleep_time = int(search.search(raw_response.text)[1])
                            time.sleep(sleep_time)
                        else:
                            logger.error(
                                "Failed to Harvest ArXiV records for daterange: {}".format(
                                    self.daterange
                                )
                            )
                            raise requests.exceptions.Timeout
                    else:
                        success = True

                except Exception as e:
                    raise e
        except Exception as e:
            raise e

        arxivparser = arxiv.MultiArxivParser()
        return arxivparser.parse(self.raw_xml)

    def __iter__(self):
        """
        Iterate through all parsed records.
        If next(self.parsed_records) fails, we attempt to extract a resumptionToken and then rerun the harvest process with the token.

        return:
            record: (str) XML of a single ArXiv record
        """

        while not self.fullHarvest:
            for record in self.parsed_records:
                yield record
            try:
                resumptionToken = self.extract_resumptionToken(self.raw_xml)
            except Exception:
                resumptionToken = None
                logger.debug("No resumptionToken present")
            if resumptionToken:
                self.parsed_records = self.harvest_arxiv(resumptionToken=resumptionToken)
            else:
                self.fullHarvest = True

    @staticmethod
    def extract_resumptionToken(raw_xml):
        """
        extracts the resumptionToken from the raw response. (ArXiv uses these for paging, see OAI harvesting docs for details).

        raw_xml: (str) XML content from ArXiv

        return: (str) ArXiv resumptionToken
        """
        arxiv_parser = arxiv.MultiArxivParser()
        token_text = next(
            arxiv_parser.get_chunks(raw_xml, r"<resumptionToken", r"</resumptionToken>")
        )
        pattern = re.compile(r"[0-9]+\|[0-9]+")
        return pattern.search(token_text)[0]
