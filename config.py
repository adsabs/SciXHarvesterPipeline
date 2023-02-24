#LOGGING_LEVEL = 'WARN'
#LOGGING_LEVEL = 'DEBUG'
LOGGING_LEVEL = 'INFO'
LOG_STDOUT = True
#SQLALCHEMY Configuration
SQLALCHEMY_URL = 'postgres://harvester:harvester@postgres:5432/harvester'
SQLALCHEMY_ECHO = False
#REDIS Configuration
REDIS_HOST = 'redis'
REDIS_PORT = 6379
#Kafka Configuration
KAFKA_BROKER = 'kafka:9092'
SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
#Harvester AVRO Schema Parameters
SCHEMA_ID = '1'
HARVESTER_INPUT_SCHEMA= 'Harvester'
HARVESTER_INPUT_TOPIC = 'HarvesterInput'
HARVESTER_OUTPUT_SCHEMA = 'HarvesterOutputSchema'
HARVESTER_OUTPUT_TOPIC = 'HarvesterOutput'
#AWS Configuration
AWS_ACCESS_KEY_ID = 'CHANGEME'
AWS_SECRET_ACCESS_KEY = 'SECRETS'
AWS_DEFAULT_REGION = 'us-east-1'
PROFILE_NAME = 'SESSION_PROFILE'
#AWS Bucket Parameters
BUCKET_NAME = 'BUCKETNAME'
BUCKET_ARN = 'BUCKETARN'
#OAI Harvesting urls
ARXIV_OAI_URL='https://export.arxiv.org/oai2'