from concurrent import futures
from unittest import TestCase

import grpc

from API.grpc_modules import harvester_grpc


def HarvesterServer(cls):
    """Instantiate a harvester server and return a stub for use in tests"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    harvester_grpc.add_HarvesterInitServicer_to_server(cls(), server)
    port = server.add_insecure_port("[::]:0")
    server.start()

    try:
        with grpc.insecure_channel("localhost:%d" % port) as channel:
            yield harvester_grpc.HarvesterInitStub(channel)
    finally:
        server.stop(None)


class TestHarvesterServer(TestCase):
    def test_Harvester_server(self):
        # may do something extra for this mock if it's stateful
        class FakeHarvesterServer(harvester_grpc.HarvesterInitServicer):
            def initHarvester(self, request, context):
                return request

        # with HarvesterServer(FakeHarvesterServer) as stub:
        #     response = stub.SayHello(helloworld_pb2.HelloRequest(name='Jack'))
        #     self.assertEqual(response.message, 'Hello, Jack!')
