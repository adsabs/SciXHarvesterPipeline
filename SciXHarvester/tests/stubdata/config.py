# LOGGING_LEVEL = 'WARN'
# LOGGING_LEVEL = 'DEBUG'
LOGGING_LEVEL = "INFO"
LOG_STDOUT = True
# SQLALCHEMY Configuration
SQLALCHEMY_URL = "postgresql://harvester:harvester@localhost:5432/harvester"
SQLALCHEMY_ECHO = False
# REDIS Configuration
REDIS_HOST = "redis"
REDIS_PORT = 6379
# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
# Harvester AVRO Schema Parameters
HARVESTER_INPUT_SCHEMA = "HarvesterInputSchema"
HARVESTER_INPUT_TOPIC = "HarvesterInput"
HARVESTER_OUTPUT_SCHEMA = "HarvesterOutputSchema"
HARVESTER_OUTPUT_TOPIC = "HarvesterOutput"
# S3 Configuration
S3_PROVIDERS = ["AWS"]
# AWS Configuration
AWS_DEFAULT_REGION = "us-east-1"
AWS_BUCKET_NAME = "test-bucket-name"
# OAI Harvesting urls
ARXIV_OAI_URL = "https://export.arxiv.org/oai2"
