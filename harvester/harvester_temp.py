from confluent_kafka import Consumer
from confluent_kafka import Producer
from datetime import datetime
import time
import random
import logging

def _consume_from_topic(consumer):
    logger.debug("Consuming from Harvester Topic")
    return consumer.poll()


def Harvester_task(consumer, producer):
    while True:

        msg = _consume_from_topic(consumer)
        if msg:
            Finish = False
            tstamp = datetime.now()
            logger.debug("Received message {}".format(msg.value()))
            producer.produce(topic='gRPC', value=b'This message was generated by the Harvester and was read from the gRPC topic %s.' % bytes(str(tstamp), 'utf-8'))
            logger.debug(b'This message was generated by the Harvester and was read from the gRPC topic %s.' % bytes(str(tstamp), 'utf-8'))
            for i in range(0,10):
                time.sleep(5)
                tstamp = datetime.now()
                if random.random() < 0.6 and b'Error' in msg.key(): 
                    producer.produce(topic='gRPC', value=b'Error') 
                    logger.info(b'Error')
                    Finish = True
                    break
                producer.produce(topic='gRPC', value=b'This message was generated by the Harvester and was read from the gRPC topic %s.' % bytes(str(tstamp), 'utf-8'))
                logger.info(b'This message was generated by the Harvester and was read from the gRPC topic %s.' % bytes(str(tstamp), 'utf-8'))
            if not Finish:
                producer.produce(topic='gRPC', value=b'Done %s.' % bytes(str(tstamp), 'utf-8'))
                tstamp = datetime.now()            
                logger.info(b'Done %s.' % bytes(str(tstamp), 'utf-8'))
        else:
            logger.info("No new messages")
            time.sleep(2)
            continue

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger=logging.getLogger(__name__)
    logger.info("Starting Harvester Service")
    consumer = Consumer({'bootstrap.servers':'kafka:9092', 'auto.offset.reset':'latest', 'group.id': 'HarvesterPipeline1'})
    consumer.subscribe(['Harvester'])
    producer = Producer({'bootstrap.servers':'kafka:9092'})
    Harvester_task(consumer, producer)