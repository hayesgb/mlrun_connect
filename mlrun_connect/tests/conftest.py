import pytest
import requests

from ..azure import AzureSBToMLRun
import v3io.dataplane


@pytest.fixture(autouse=True)
def no_servicebus(monkeypatch):
    """Remove service bus connection"""
    monkeypatch.delattr("AzureSBToMLRun.do_connect")

@pytest.fixture
def mock_v3io_table(monkeypatch):
    class MockV3ioClient:
        def __init__(self, *args, **kwargs):
            pass
        
        def create_schema(self, *args, **kwargs):
            pass

    monkeypatch.setattr(requests, "get", None)
    monkeypatch.setattr(requests, "head", None)
    monkeypatch.setattr(v3io.dataplane, "Client", MockV3ioClient)
    
