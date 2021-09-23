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

@pytest.fixture
def mock_initial_message():
    return {
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
    mocker.patch.object(handler, "_listener_thread", return_value=True)
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


def test_process_initial_message(mock_initial_message, mocker):
    """
    Verify that the initially processed message gets handled
    as expected in the kv store
    """
 
    def _verify_cnxn(request):
        return MagicMock(status_code=200)
    
    def _verify_response(request):
        output = MagicMock(items={})
        return MagicMock(status_code=200,
                         output=output)
    
    def _respond_put_accepted(request):
        return MagicMock(status_code=200)

    def _count_running_pipelines_zero(request):
        output = MagicMock(items = [])
        return MagicMock(output=output)
    
    def _update_kv_data(request):
        return MagicMock(status_code=200)

    verifier_transport = v3io.dataplane.transport.verifier.Transport(
        request_verifiers=[_verify_cnxn, _verify_response, _respond_put_accepted,
                           _count_running_pipelines_zero, _update_kv_data]
    )
    handler = AzureSBToMLRun(
        queue_name="myqueue",
        connection_string="None",
        transport_kind=verifier_transport,
    )
    mocker.patch.object(handler, "_listener_thread", autospec=True)
    mocker.patch.object(handler, "get_servicebus_receiver", autospec=True)
    message = mock_initial_message
    print(message)
    sb_message_id = list(message.keys())[0]
    print(sb_message_id)

    # Assume the message is not available in the kv store
    result = handler.check_kv_for_message(sb_message_id=sb_message_id)
    assert result == (False, {})
    
    # Assume the count of running pipelines in the kv store is 0
    # Add the message to the kv store
    mocker.patch.object(handler, "run_pipeline", return_result="mock_workflow_id")
    