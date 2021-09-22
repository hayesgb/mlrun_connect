from os import stat
import pytest
import requests

from mlrun_connect.azure import AzureSBToMLRun
import v3io.dataplane.transport


# @pytest.fixture(autouse=True)
# def no_servicebus(monkeypatch):
#     """Remove service bus connection"""
#     monkeypatch.delattr("mlrun_connect.azure.AzureSBToMLRun.do_connect")
