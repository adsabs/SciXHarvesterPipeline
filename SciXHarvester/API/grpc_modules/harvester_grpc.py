"""Client and server classes for gRPC services."""
import grpc


class HarvesterInitStub(object):
    """The greeting service definition."""

    def __init__(self, channel, avroserialhelper):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.initHarvester = channel.unary_stream(
            "/harvesteraapi.HarvesterInit/initHarvester",
            request_serializer=avroserialhelper.avro_serializer,
            response_deserializer=avroserialhelper.avro_deserializer,
        )


class HarvesterInitServicer(object):
    """The greeting service definition."""

    def initHarvester(self, request, context):
        """Initializes requests to the Harvester and monitors the results."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_HarvesterInitServicer_to_server(servicer, server, avroserialhelper):
    rpc_method_handlers = {
        "initHarvester": grpc.unary_stream_rpc_method_handler(
            servicer.initHarvester,
            request_deserializer=avroserialhelper.avro_deserializer,
            response_serializer=avroserialhelper.avro_serializer,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "harvesteraapi.HarvesterInit", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class HarvesterInit(object):
    """The greeting service definition."""

    @staticmethod
    def initHarvester(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            "/harvesteraapi.HarvesterInit/initHarvester",
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )


class HarvesterMonitorStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel, avroserialhelper):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.monitorHarvester = channel.unary_stream(
            "/harvesteraapi.HarvesterMonitor/monitorHarvester",
            request_serializer=avroserialhelper.avro_serializer,
            response_deserializer=avroserialhelper.avro_deserializer,
        )


class HarvesterMonitorServicer(object):
    """Missing associated documentation comment in .proto file."""

    def monitorHarvester(self, request, context):
        """Initializes requests to the Harvester and monitors the results."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_HarvesterMonitorServicer_to_server(servicer, server, avroserialhelper):
    rpc_method_handlers = {
        "monitorHarvester": grpc.unary_stream_rpc_method_handler(
            servicer.monitorHarvester,
            request_deserializer=avroserialhelper.avro_deserializer,
            response_serializer=avroserialhelper.avro_serializer,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "harvesteraapi.HarvesterMonitor", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class HarvesterMonitor(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def monitorHarvester(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            "/harvesteraapi.HarvesterMonitor/monitorHarvester",
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
