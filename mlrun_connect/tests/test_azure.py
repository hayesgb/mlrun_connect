from unittest.mock import patch, MagicMock
import pytest

from mlrun_connect.azure import AzureSBToMLRun
import v3io.dataplane


@pytest.fixture
def mock_azure_servicebus_message():
    return {
        "id": "012345",
        "messageTopic": "MockTopic",
        "eventType": "MockEvent",
        "eventTime": "2021-09-13T07:05:30.7930744Z",
        "data": {
            "blobUrl": "https://testaccount.blob.core.windows.net/testcontainer/test_file.txt"
        },
    }


def test_parse_servicebus_message(mock_azure_servicebus_message, mocker):
    """
    Verify the expected output from parsing a message from Service Bus
    """

    def _verify_cnxn(request):
        return MagicMock(status_code=200)

    verifier_transport = v3io.dataplane.transport.verifier.Transport(
        request_verifiers=[_verify_cnxn]
    )

    handler = AzureSBToMLRun(
        queue_name="myqueue",
        connection_string="None",
        transport_kind=verifier_transport,
    )
    # mocker.patch("mlrun_connect.azure.AzureSBToMLRun.get_servicebus_receiver")
    mocker.patch.object(handler, "get_servicebus_receiver", autospec=True)

    parsed_message = handler.parse_servicebus_message(
        message=mock_azure_servicebus_message
    )
    expected_result = {
        "012345": {
            "abfs_account_name": "testaccount",
            "abfs_path": "az://testcontainer/test_file.txt",
            "sb_message_topic": "MockTopic",
            "blob_url": "https://testaccount.blob.core.windows.net/testcontainer/test_file.txt",
            "run_attempts": 0,
            "run_start_timestamp": "null",
            "run_status": "null",
            "sb_event_time": "2021-09-13T07:05:30.7930744Z",
            "sb_event_type": "MockEvent",
            "sb_message_topic": "MockTopic",
            "workflow_id": "null",
        }
    }
    assert parsed_message == expected_result
