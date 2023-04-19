import pytest

class mock_gRPC_avro_msg():
    def value(self):
        return {'hash': 'g425897fh3qp35890u54256342ewferht242546', 'id': None, 'task': 'ARXIV', 'status': None, 'task_args': {'ingest': None, 'ingest_type': 'metadata', 'daterange': '2023-03-07', 'resumptionToken': None, 'persistence': None}}
    def bitstream(self):
        return b'\x00Ng425897fh3qp35890u54256342ewferht242546\x02\x00\x00\x00\x02\x10metadata\x02\x142023-03-07\x00\x02'