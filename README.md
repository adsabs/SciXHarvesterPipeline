[![Python CI actions](https://github.com/tjacovich/ADSHarvesterPipeline/actions/workflows/python_actions.yml/badge.svg)](https://github.com/tjacovich/ADSHarvesterPipeline/actions/workflows/python_actions.yml) [![Coverage Status](https://coveralls.io/repos/github/tjacovich/SciXHarvesterPipeline/badge.svg?branch=main)](https://coveralls.io/github/tjacovich/SciXHarvesterPipeline?branch=main)
# Harvester Repository

![Harvester Pipeline Flowchart](README_assets/Harvester_implementation.png?raw=true "Harvester Pipeline Flowchart")

## Setting Up a Development Environment.
### Installing dependencies and hooks
This project uses `pyproject.toml` to install necessary dependencies and otherwise set up a working development environment. To set up a local working environment, simply run the following:
```bash
virtualenv .venv
source .venv/bin/activate
pip install .[dev]
pip install .
pre-commit install
pre-commit install --hook-type commit-msg
```

To install gRPC, the following variables may need to be set in order for the build to succeed:

```
export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1
export GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1
```

### Testing with pytest
Tests can be run from  the `SciXHarvester` directory using pytest:
```bash
cd SciXHarvester/
py.test
```

### Testing Against Kafka
#### The Kafka Environment
In order to set up a full development environment, a kafka instance must be created that contains at least:
 - kafka broker
 - kafka zookeeper
 - kafka schema registry
 - postgres
 - redis
 - minIO (or AWS S3 bucket)

The following can also be helpful:
 - kafka-ui
 - pgadmin

Next, we need to copy `config.py` to `local_config.py` and update the environment variables to point to reflect the values of the local environment.
For `postgres`, we will need a  database to store harvester data. We will also need an `S3` bucket created either on `AWS` or locally on a `minIO` instance.
We will also need to create Harvester input and output topics which can be done either through python or by using the `kafka-ui`.
The relevant `AVRO` schemas from `SciXHarvester/AVRO_schemas/` must also be added to the schema registry using either python or the UI.

#### Launching The Harvester
The harvester requires `librdkafka` be installed. The source can be found [here](https://github.com/edenhill/librdkafka).
Installation on most `nix systems can be done by running the following:
```bash
git clone https://github.com/edenhill/librdkafka
cd librdkafka && ./configure && make && make install && ldconfig
```
To launch the harvester, the following commands need to be run within the `SciXHarvester/` directory
```bash
#Start gRPC API
python3 run.py HARVESTER_API
#Start Harvester pipeline consumer and producer
python3 run.py HARVESTER_APP
```

## Sending commands to the gRPC API

Currently, there are two methods that have been defined in the API for interacting with the Harvester Pipeline.
- `HARVESTER_INIT`: Initialize a job with given `job_args` passed into the script as a JSON.
- `HARVESTER_MONITOR`: Queries the status of a job with a given `<job_id>`

Additionally, calling either command with `--persistence` will open a persistent connection that streams updates for the specificed job.

```bash
#This command tells the server to initialize a job by adding a message to the Harvester Topic
python3 API/harvester_client.py HARVESTER_INIT --task "ARXIV" --task_args '{"harvest_type": "metadata", "daterange": "YYYY-MM-DD"}'
#This command asks the server to check on the current status of a job with <job_id>
python3 API/harvester_client.py HARVESTER_MONITOR --job_id '<job_id>'
```


## Maintainers

Taylor Jacovich
