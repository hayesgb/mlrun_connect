import datetime
import os
import json
import logging
from pathlib import Path
import time
from threading import Thread
from urllib.parse import urlparse

from azure.identity import ClientSecretCredential
from azure.servicebus import ServiceBusClient
from azure.servicebus.exceptions import (
    ServiceBusAuthorizationError,
    ServiceBusError,
    MessageAlreadySettled,
    MessageLockLostError,
    MessageNotFoundError,
)
from mlrun import get_run_db
import v3io.dataplane


class AzureSBToMLRun:
    """
    Listen in the background for messages on a Azure Service Bus Queue
    (like Nuclio).  If a message is received, parse the message and
    use it to start a Kubeflow Pipeline.
    The current design expects to receive a message that was sent to the
    Queue from Azure Event Grid.

    This is leveraged by installing this package in the Nuclio image a build
    time, and importing the package into Nuclio.  A new class is created inside
    the Nuclio function's init_context, with this object as the parent class.
    From here, the run_pipeline method should be overridden by a custom
    run_pipeline function that dictates how to start the execution of a
    Kubeflow Pipeline.
    This method will receive and event, which is the parsed message
    from Azure Service Bus, and should return an workflow_id

    Example
    -------
    import time

    from src.handler import AzureSBQueueToNuclioHandler
    from mlrun import load_project

    def init_context():
        pipeline = load_project(<LOAD MY PROJECT HERE>)
        class MyHandler(AzureSBQueuToNuclioHandler):
            def run_pipeline(self, event):
                i = 0
                try:
                    arguments = {
                        "account_name": event.get("abfs_account_name"),
                        "abfs_file": event.get("abfs_path")
                    }
                    workflow_id = pipeline.run(arguments = arguments)
                except Exception as e:
                    # if this attempt returns an exception and retry logic
                    i += 1
                    if i >= 3:
                        raise RuntimeError(f"Failed to start pipeline for {e}")
                    time.sleep(5)
                    self.run_pipeline(event)
                return workflow_id


    This class also stores the run data in a V3IO key-value store, and

    Parameters
    ----------
    credential
        A credential acquired from Azure
    connection_string
        An Azure Service Bus Queue Connection String
    queue_name
        The queue on which to listen
    tenant_id
        The Azure tenant where your Service Bus resides
    client_id
        The Azure client_id for a ServicePrincipal
    client_secret
        The secret for your Azure Service Principal
    credential
        Any credential that can be provided to Azure for authentication
    connection_string
        An Azure connection string for your Service Bus
    mlrun_project
        This is the name of the mlrun project that will be run
        By default this is pulled from environmental variables.  Otherwise
        input will be taken from here, or be designated as "default"

    Users can authenticate to the Service Bus using a connection string and
    queue_name, or using a ClientSecretCredential, which requires also
    providing the namespace of your Service Bus, and the tenant_id,
    client_id, and client_secret
    """

    def __init__(
        self,
        queue_name,
        namespace=None,
        tenant_id=None,
        client_id=None,
        client_secret=None,
        credential=None,
        connection_string=None,
        max_concurrent_pipelines=3,
        mlrun_project=None,
        transport_kind=None,
    ):
        self.credential = credential
        self.namespace = namespace
        self.sb_client = None
        self.tenant_id = (
            tenant_id
            or os.getenv("AZURE_TENANT_ID")
            or os.getenv("AZURE_STORAGE_TENANT_ID")
        )
        self.client_id = (
            client_id
            or os.getenv("AZURE_CLIENT_ID")
            or os.getenv("AZURE_STORAGE_CLIENT_ID")
        )
        self.client_secret = (
            client_secret
            or os.getenv("AZURE_CLIENT_SECRET")
            or os.getenv("AZURE_STORAGE_CLIENT_SECRET")
        )
        self.connection_string = connection_string
        self.max_concurrent_pipelines = max_concurrent_pipelines
        self.servicebus_queue_name = queue_name
        self.v3io_container = "projects"
        self.project = os.getenv("MLRUN_DEFAULT_PROJECT") or mlrun_project or "default"
        self.table = os.path.join(self.project, "servicebus_table")
        if (
            self.credential is None
            and self.tenant_id is not None
            and self.client_id is not None
            and self.client_secret is not None
        ):
            self.credential = self._get_credential_from_service_principal()
        self.v3io_client = v3io.dataplane.Client(
            max_connections=1, transport_kind=transport_kind or "httpclient"
        )
        self.tbl_init()
        self._listener_thread = Thread(target=self.do_connect, daemon=True)
        self._listener_thread.start()

    def _get_credential_from_service_principal(self):
        """
        Create a Credential for authentication.  This can include a
        TokenCredential, client_id, client_secret, and tenant_id
        """
        credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return credential

    def do_connect(self):
        """Create a connection to service bus"""
        logging.info("do_connect")
        while True:
            if self.connection_string is not None:
                self.sb_client = ServiceBusClient.from_connection_string(
                    conn_str=self.connection_string
                )
            elif self.namespace is not None and self.credential is not None:
                self.fqns = f"https://{self.namespace}.servicebus.windows.net"
                self.sb_client = ServiceBusClient(self.fqns, self.credential)
            else:
                raise ValueError("Unable to create connection to Service Bus!")
            self.get_servicebus_receiver()

    def tbl_init(self, overwrite=False):
        """
        If it doesn't exist, create the v3io table and k,v schema

        :param overwrite: Manually overwrite the
        """

        if (not Path("/v3io", self.v3io_container, self.table).exists()) or (
            overwrite is True
        ):
            logging.info("Creating table.")
            self.v3io_client.create_schema(
                container=self.v3io_container,
                path=self.table,
                key="sb_message_id",
                fields=[
                    {"name": "abfs_account_name", "type": "string", "nullable": True},
                    {"name": "abfs_path", "type": "string", "nullable": True},
                    {"name": "blob_url", "type": "string", "nullable": True},
                    {"name": "run_attempts", "type": "long", "nullable": False},
                    {
                        "name": "run_start_timestamp",
                        "type": "timestamp",
                        "nullable": True,
                    },
                    {"name": "run_status", "type": "string", "nullable": True},
                    {"name": "sb_event_time", "type": "timestamp", "nullable": True},
                    {"name": "sb_event_type", "type": "string", "nullable": True},
                    {"name": "sb_message_id", "type": "string", "nullable": False},
                    {"name": "sb_message_topic", "type": "string", "nullable": True},
                    {"name": "workflow_id", "type": "string", "nullable": True},
                ],
            )
        else:
            logging.info("table already exists.  Do not recreate")

    def get_servicebus_receiver(self):
        """Construct the service bus receiver"""
        with self.sb_client as client:
            receiver = client.get_queue_receiver(queue_name=self.servicebus_queue_name)
            self.receive_messages(receiver)

    def receive_messages(self, receiver):
        should_retry = True
        while should_retry:
            with receiver:
                try:
                    for msg in receiver:
                        try:
                            logging.info("get message")
                            message = json.loads(str(msg))
                            # Parse the message from Service Bus into a usable format
                            parsed_message = self.parse_servicebus_message(message)
                            # Add the message to the kv store and start a pipeline
                            self.process_message(parsed_message)
                            should_complete = True
                        except Exception as e:
                            logging.info(
                                f"There an exception for {e}!"
                                "Do not complete the message"
                            )
                            should_complete = False
                        for _ in range(3):  # settlement retry
                            try:
                                if should_complete:
                                    logging.info("Complete the message")
                                    receiver.complete_message(msg)
                                else:
                                    logging.info("Skipped should_complete")
                                    #
                                    break
                            except MessageAlreadySettled:
                                # Message was already settled.  Continue
                                logging.info("message already settled")
                                break
                            except MessageLockLostError:
                                # Message lock lost before settlemenat.
                                # Handle here
                                logging.info("message lock lost")
                                break
                            except MessageNotFoundError:
                                # Message does not exist
                                logging.info("Message not found")
                                break
                            except ServiceBusError:
                                # Undefined error
                                logging.info("SB Error")
                                continue
                    return
                except ServiceBusAuthorizationError:
                    # Permission error
                    raise
                except:  # NOQA E722
                    continue

    def check_kv_for_message(self, sb_message_id):
        """
        Check to see if an entry with the specified Azure Service Bus
        If the message_id is present, return True, and the attributes
        from the key-value store

        :param message_id: The message to be interrogated
        :returns A tuple of True/False and if True, the k,v run status
            from the table

        """
        query = f"sb_message_id == '{sb_message_id}'"
        print(f"is message_id:  {sb_message_id} in kv store?")
        response = self.v3io_client.kv.scan(
            container=self.v3io_container,
            table_path=self.table,
            filter_expression=query,
        )
        print(f"response:  {response}")
        items = response.output.items

        if not items:
            logging.info("sb_message_id not in the kv store")
            return False, items
        elif len(items) == 1:
            item = items[0]
            logging.info("sb_message_id is in kv store")
            return True, item
        else:
            raise ValueError("Found duplicate entries by message_id in k,v store!")

    def update_kv_data(self, message, action=None):
        """Add the Service Bus message to the kv table"""

        try:
            if action in ["create_entry", "update_entry", "delete_entry"]:
                if action == "create_entry":
                    logging.info("Adding record")
                    for item_key, item_attributes in message.items():
                        self.v3io_client.kv.put(
                            container=self.v3io_container,
                            table_path=self.table,
                            key=item_key,
                            attributes=item_attributes,
                        )
                elif action == "update_entry":
                    logging.info("Updating record")
                    for item_key, item_attributes in message.items():
                        self.v3io_client.kv.update(
                            container=self.v3io_container,
                            table_path=self.table,
                            key=item_key,
                            attributes=item_attributes,
                        )
                elif action == "delete_entry":
                    logging.info("Removing record")
                    for item_key, item_attributes in message.items():
                        self.v3io_client.kv.delete(
                            container=self.v3io_container,
                            table_path=self.table,
                            key=item_key,
                        )
                else:
                    raise ValueError("Value passed to update_kv_data unknown!")

        except Exception as e:
            raise RuntimeError(f"Failed to add message to kv for {e}")

    def run_pipeline(self, event: dict):
        """
        This is the method that starts the execution of the pipeline.  It
        should be overridden in the Nuclio function

        :param event: The message that was sent by Service Bus,
            after being parsed.  It can be used to provide arguments that are
            passed to the pipeline
        """
        pass

    def _parse_blob_url_to_fsspec_path(self, blob_url):
        """
        Convert the blob_url to fsspec compliant format and account
        information

        :param blob_url: For a createBlob event, this is the blobUrl
            sent in the message
        :returns A tuple of the Azure Storage Blob account name and a
            fsspec compliant filepath
        """

        url_components = urlparse(blob_url)
        path = url_components.path.strip("/")
        account_name = url_components.netloc.partition(".")[0]
        abfs_path = f"az://{path}"
        return account_name, abfs_path

    def count_running_pipelines(self):
        """
        Get a count of the pipelines in KV that are in a running
        or started state
        """
        query = "run_status in ('Started', 'Running')"

        response = self.v3io_client.kv.scan(
            container=self.v3io_container,
            table_path=self.table,
            filter_expression=query,
        )
        items = response.output.items
        return len(items)

    def get_run_status(self, workflow_id):
        """
        Retrieves the status of a pipeline run from the mlrun database

        :param workflow_id: A workflow_id from the mlrun database
        :return A tuple of the run status of a pipeline, and the pipeline start timestamp
        """
        try:
            db = get_run_db().connect()
            pipeline_info = db.get_pipeline(run_id=workflow_id)
            run_status = pipeline_info.get("run").get("status") or "none"
            data = json.loads(
                pipeline_info.get("pipeline_runtime").get("workflow_manifest")
            )
            run_start_ts = data.get("status").get("startedAt")
            if run_start_ts is None:
                run_start_ts = "null"
            else:
                run_start_ts = datetime.strptime(run_start_ts, "%Y-%m-%dT%H:%M:%SZ")

            return run_status, run_start_ts
        except Exception as e:
            raise RuntimeError(f"Failed to get_run_status for {e}")

    def parse_servicebus_message(self, message):
        """
        Write the logic here to parse the incoming message.

        :param message: A Python dict of the message received from
            Azure Service Bus
        :returns A nested Python dict with information information from the
            Service Bus message for handling a pipeline run, and for tracking
            that run as it flows through the pipeline.
        """
        logging.info("Process the incoming message.")
        # logging.info(message)
        sb_message_id = message.get("id", None)
        if sb_message_id is None:
            raise ValueError("Unable to identify sb_message_id!")
        sb_message_topic = message.get("messageTopic", "null")
        sb_event_type = message.get("eventType", "null")
        sb_event_time = message.get("eventTime", "null")
        data = message.get("data", "null")
        if data != "none":
            blob_url = data.get("blobUrl", "null")
            # Reformat the blob_url to a fsspec-compatible file location
            abfs_account, abfs_path = self._parse_blob_url_to_fsspec_path(blob_url)

        parsed_message = {
            sb_message_id: {  # The messageId from Service Bus
                "sb_message_topic": sb_message_topic,  # messageTopic from Service Bus
                "sb_event_type": sb_event_type,  # The eventType from Service Bus
                "sb_event_time": sb_event_time,  # The eventTime from service Bus
                "blob_url": blob_url,  # The blobUrl -- The blob created
                "workflow_id": "null",  # This is the workflow_id set by mlrun
                "run_status": "null",  # This is the run_status in Iguazio
                "run_attempts": 0,  # zero-index count the number of run attempts
                "abfs_account_name": abfs_account or "null",
                "abfs_path": abfs_path or "null",
                "run_start_timestamp": "null",
            }
        }

        return parsed_message

    def process_message(self, message):
        """
        Write the logic here to process the parsed message.  Validate it is not present
        in the KV store, and start a Kubeflow Pipeline

        :param message: A Python dict of the parsed message from Azure Service Bus
        """
        sb_message_id = list(message.keys())[0]

        # Check to see if the message_id is in the nosql run table
        has_message, existing_kv_entry = self.check_kv_for_message(sb_message_id)
        if not has_message:
            # If the message_id is not in the k,v store, Add it
            # to the store
            logging.info("message_id not found in kv store")
            self.update_kv_data(message, action="create_entry")
            # Check to see if the number of running pipelines exceeds the allowable
            # concurrent pipelines.
            num_running_pipelines = self.count_running_pipelines()
            logging.info(f"There are {num_running_pipelines} returned from kv")
            if num_running_pipelines < self.max_concurrent_pipelines:
                workflow_id = self.run_pipeline(event=message)
                run_status = "Started"
                logging.info(f"workflow_id is:  {workflow_id}")
                if workflow_id != "none":
                    # Here we're starting the pipeline and adding the workflow_id
                    # to the kv store
                    logging.info(f"workflow_id is:  {workflow_id}")
                    message[sb_message_id]["workflow_id"] = workflow_id
                    message[sb_message_id]["run_status"] = run_status
            self.update_kv_data(message, action="update_entry")
            logging.info("Sleeping")
            time.sleep(20)

    #         else:
    #             logging.info(
    #                 "Found message_id in the kv store.  Check to see if the "
    #                 "pipeline is running or has run"
    #             )
    #             run_status = existing_kv_entry[message_id]["run_status"]
    #             if run_status in ["none", "Failed"]:
    #                 workflow_id = self.run_pipeline(event=existing_kv_entry)
    #                 run_status = "Started"
    #                 existing_kv_entry[message_id]["workflow_id"] = workflow_id
    #                 existing_kv_entry[message_id]["run_status"] = run_status
    #                 self.update_kv_data(existing_kv_entry, action="update_entry")
    #             elif run_status == "Running":
    #                 pass
    #             else:
    #                 logging.info(f"run_status unknown:  {run_status}")

    def check_and_update_run_status(self, ttl: int = 4):
        """
        This will be run in the Nuclio handler on a CRON trigger.
        We will retrieve a list of active runs from the KV store and
        check their status.  We can update the kv store if a run is done.

        :param ttl: Amount of time, in hours, to allow a run to remain in a running
            state before sending a kill signal
        """

        # Find any entries in the kv store with no status or in a running state
        query = "run_status in ('Started', 'Running')"

        # And calculate the current time, so we can decide how to handle
        # long-running jobs and if we should ignore old runs
        now = datetime.datetime.now(datetime.timezone.utc)
        ttl = ttl * 3600  # Convert hours to seconds

        # Get the count of running pipelines in the KV store
        # as well as the pipelines in KV that are logged as in progress
        num_running_pipelines = self.count_running_pipelines()
        logging.info(f"How many running pipelines were found:  {num_running_pipelines}")
        try:
            response = self.v3io_client.kv.scan(
                container=self.v3io_container,
                table_path=self.table,
                filter_expression=query,
            )
            items = response.output.items
            if items:
                # If there are runs in the kv store that are in a started
                # or running state, check their run_status

                for item in items:
                    logging.info(item)
                    new_item = {}
                    workflow_id = item["workflow_id"]
                    sb_message_id = item.pop("__name")
                    new_item[sb_message_id] = item
                    logging.info(f"Checking run info for workflow_id:" f"{workflow_id}")

                    # Get the latest run status for the workflow_id from the mlrun database
                    # and update it here.
                    run_status, run_start_ts = self.get_run_status(workflow_id)
                    new_item[sb_message_id]["run_status"] = run_status
                    new_item[sb_message_id]["run_start_timestamp"] = run_start_ts
                    if run_status == "Succeeded":
                        logging.info("run_status is 'Succeeded'")
                        pass
                    elif (run_status in ["Failed", "none"]) and (
                        num_running_pipelines <= self.max_concurrent_pipelines
                    ):  # and current_run_info["run_attempts"] < 3:
                        logging.info(
                            "Run status in a Failed or none state."
                            "Retry the pipeline!"
                        )
                        if run_status == "Failed":
                            new_item[sb_message_id]["run_attempts"] += 1
                        workflow_id = self.run_pipeline(new_item)
                        run_status, run_start_ts = self.get_run_status(workflow_id)
                        new_item[sb_message_id]["workflow_id"] = workflow_id
                        new_item[sb_message_id]["run_status"] = run_status
                        new_item[sb_message_id]["run_start_timestamp"] = run_start_ts
                    elif run_status in ["Running", "Started"]:
                        logging.info("KV shows run in progress.  Update status...")
                        run_status, run_start_ts = self.get_run_status(workflow_id)
                        # Check to see if the run has been going excessively long.
                        # If so, kill the run and retry
                        pipeline_runtime = (now - run_start_ts).seconds
                        if pipeline_runtime > ttl:
                            try:
                                db = get_run_db.connect()
                                db.abort_run(workflow_id)
                            except Exception as e:
                                logging.info(f"Failed to abort run with {e}")

                        new_item[sb_message_id]["run_attempts"] += 1
                        if new_item[sb_message_id]["run_attempts"] <= 3:
                            db = get_run_db().connect()
                            pipe = db.get_pipeline(run_id=workflow_id)
                            pipe = json.loads(
                                pipe["pipeline_runtime"]["workflow_manifest"]
                            )
                            uid = pipe["metadata"]["uid"]
                            db.abort_run(uid, project=self.project)

                    else:
                        logging.info(
                            f"run_status is not known."
                            f"Got a run_status of {run_status}"
                        )
                    logging.info("Update record in kv")
                    self.update_kv_data(new_item, action="update_entry")
                    num_running_pipelines = self.count_running_pipelines()
                    logging.info("Sleeping")
                    time.sleep(10)
            else:
                logging.info(
                    "No runs found in kv store that match"
                    "'Started' or 'Running' state."
                )

        except Exception as e:
            logging.info(f"Caught exception {e}." "Update counter retry!")
            time.sleep(10)
