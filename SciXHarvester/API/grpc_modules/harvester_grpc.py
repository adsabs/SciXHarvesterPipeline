"""Client and server classes for gRPC services."""
import grpc


class HarvesterInitStub(object):
    """The Stub for connecting to the Harvester init service."""

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
    """The servicer definition for initiating jobs with the Harvester pipeline."""

    def initHarvester(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_HarvesterInitServicer_to_server(servicer, server, avroserialhelper):
    """The actual methods for sending and receiving RPC calls."""
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


class HarvesterInit(object):
    """The definition of the Harvester gRPC API and stream connections."""

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
    """The Stub for connecting to the Harvester Monitor service."""

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
    """The servicer definition for monitoring jobs already submitted to the Harvester pipeline."""

    def monitorHarvester(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_HarvesterMonitorServicer_to_server(servicer, server, avroserialhelper):
    """The actual methods for sending and receiving RPC calls."""
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


class HarvesterMonitor(object):
    """The definition of the Monitor gRPC API and stream connections."""

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
