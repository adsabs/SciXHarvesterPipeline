import datetime
import logging as logger

import harvester.models as models

logger.basicConfig(level=logger.DEBUG)


def write_status_redis(redis_instance, status):
    logger.debug("Publishing status: {}".format(status))
    redis_instance.publish("harvester_statuses", status)


def get_job_status_by_job_hash(cls, job_hashes, only_status=None):
    """
    Return all updates with job_hash
    """
    with cls.session_scope() as session:
        status = None
        logger.info("Opening Session")
        for job_hash in job_hashes:
            if only_status:
                record_db = (
                    session.query(models.gRPC_status)
                    .filter(models.gRPC_status.job_hash == job_hash)
                    .filter_by(status=only_status)
                    .first()
                )
            else:
                record_db = (
                    session.query(models.gRPC_status)
                    .filter(models.gRPC_status.job_hash == job_hash)
                    .first()
                )
            if record_db:
                status = record_db.status
                logger.info("{} has status: {}".format(record_db.job_hash, status))

    return status


def _get_job_by_job_hash(session, job_hash, only_status=None):
    """
    Return all updates with job_hash
    """
    logger.info("Opening Session")

    if only_status:
        record_db = (
            session.query(models.gRPC_status)
            .filter(models.gRPC_status.job_hash == job_hash)
            .filter_by(status=only_status)
            .first()
        )
    else:
        record_db = (
            session.query(models.gRPC_status)
            .filter(models.gRPC_status.job_hash == job_hash)
            .first()
        )
    if record_db:
        logger.info("Found record: {}".format(record_db.job_hash))
    return record_db


def write_job_status(cls, job_request, only_status=None):
    """
    Return all updates with job_hash
    """
    with cls.session_scope() as session:
        job_status = models.gRPC_status()
        job_status.job_hash = job_request.get("hash")
        job_status.job_request = job_request.get("task")
        job_status.status = job_request.get("status")
        job_status.timestamp = datetime.datetime.now()
        session.add(job_status)
        session.commit()
    return True


def update_job_status(cls, job_hash, status=None):
    """
    Return all updates with job_hash
    """
    updated = False
    with cls.session_scope() as session:
        job_status = _get_job_by_job_hash(session, job_hash)
        if job_status:
            job_status.status = status
            job_status.timestamp = datetime.datetime.now()
            session.add(job_status)
            session.commit()
            updated = True
    return updated


def write_harvester_record(cls, record_id, date, s3_key, checksum, source):
    """
    Write harvested record to db.
    """
    success = False
    with cls.session_scope() as session:
        harvester_record = models.Harvester_record()
        harvester_record.record_id = record_id
        harvester_record.s3_key = s3_key
        harvester_record.date = date
        harvester_record.checksum = checksum
        harvester_record.source = source
        session.add(harvester_record)
        session.commit()
        success = True
    return success


def get_harvester_record(cls, record_ids):
    """
    Return all updates with job_hash
    """
    with cls.session_scope() as session:
        logger.info("Opening Session")
        for record_id in record_ids:
            record_db = (
                session.query(models.Harvester_record)
                .filter(models.Harvester_record.record_id == record_id)
                .first()
            )
    return record_db
