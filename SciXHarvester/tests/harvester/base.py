import contextlib
from unittest import TestCase

import requests_mock


class base_utils(TestCase):
    @staticmethod
    @contextlib.contextmanager
    def mock_multiple_targets(mock_patches):
        """
        `mock_patches` is a list (or iterable) of mock.patch objects

        This is required when too many patches need to be applied in a nested
        `with` statement, since python has a hardcoded limit (~20).

        Based on: https://gist.github.com/msabramo/dffa53e4f29ec2e3682e
        """
        mocks = {}

        for mock_name, mock_patch in mock_patches.items():
            _mock = mock_patch.start()
            mocks[mock_name] = _mock

        yield mocks

        for mock_name, mock_patch in mock_patches.items():
            mock_patch.stop()


class mock_job_request(object):
    def value(self):
        return {
            "hash": "g425897fh3qp35890u54256342ewferht242546",
            "task_args": {
                "ingest_type": "metadata",
                "daterange": "2023-03-07",
                "resumptionToken": None,
            },
            "task": "ARXIV",
        }


class MockGetRecord(requests_mock.MockerCore):
    """
    Thin wrapper around the MockADSWSAPI class specficically for the Solr
    Query end point.
    """

    def __init__(self, **kwargs):
        requests_mock.MockerCore.__init__(self)
        self.kwargs = kwargs
        with open('tests/stubdata/arxiv_stubdata/metadata/GetRecord_data.xml', 'r+') as f:
             response_text = f.read()
        self.url='https://export.arxiv.org/oai2?verb=GetRecord&identifier=oai%3AarXiv.org%3A2107.10460'

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()


class MockListRecords(requests_mock.MockerCore):
    """
    Thin wrapper around the MockADSWSAPI class specficically for the Solr
    Query end point.
    """

    def __init__(self, **kwargs):
        requests_mock.MockerCore.__init__(self)
        self.kwargs = kwargs
        self.url = "https://export.arxiv.org/oai2"
        self.possible_params = [
            "?metadataPrefix=oai_dc&from=2023-03-07&verb=ListRecords",
            "?verb=ListRecords&resumptionToken=6511260%7C1001",
            "?verb=ListRecords&resumptionToken=6511260%7C2001",
        ]
        status = 200
        for i in range(0, len(self.possible_params)):
            if self.kwargs.get("error_503"):
                status = 503
            self.register_uri(
                "GET",
                self.url + str(self.possible_params[i]),
                text=self.callback(i, self.kwargs),
                status_code=status,
            )

    def callback(self, i, kwargs):
        if not kwargs.get('error_503'):
            with open('tests/stubdata/arxiv_stubdata/metadata/ListRecords_data_{}.xml'.format(i), 'r+') as f:
                response_text = f.read()
        else:
            with open('tests/stubdata/arxiv_stubdata/metadata/arxiv_retry_after.html', 'r+') as f:
                response_text = f.read()  
        return response_text

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()


class MockListIdentifiers(requests_mock.MockerCore):
    """
    Thin wrapper around the MockADSWSAPI class specficically for the Solr
    Query end point.
    """

    def __init__(self, **kwargs):
        requests_mock.MockerCore.__init__(self)
        self.kwargs = kwargs
        with open('tests/stubdata/arxiv_stubdata/metadata/ListIdentifiers_data.xml', 'r+') as f:
             response_text = f.read()
        self.url='https://export.arxiv.org/oai2?metadataPrefix=oai_dc&verb=ListIdentifiers&from=2023-03-07'

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()
