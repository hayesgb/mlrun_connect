import pytest

from ..azure import AzureSBToMLRun


def test_receive_message(monkeypatch):
    """
    Given a monkeypatched version of AzureSBToMLRun,
    receive a message
    """
    
    def mock_