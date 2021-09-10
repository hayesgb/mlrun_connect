import concurrent.futures
import os
import json
import logging
import pandas as pd
import time
import threading
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
import v3io_frames as v3f


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
        mlrun_project=None,
    ):
        self.credential = credential
        self.namespace = namespace
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
        self.queue_name = queue_name
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
        #         self._tbl_init()
        self.v3io_client = v3f.Client(
            address="framesd:8081", container=self.v3io_container
        )
        self._listener_thread = threading.Thread(target=self.do_connect, daemon=True)
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
        """ Create a connection to service bus"""
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

    #     def _tbl_init(self):
    #         """ Create the v3io table and it's schema """
    #     client = v3io.dataplane.Client(max_connections=1)
    #     client.create_schema(
    #      container = self.v3io_container,
    #       path =self.table,
    #         fields=[
    #             {"name": "message_id", "type": "string", "nullable": False},
    #             {"name": "message_topic", "type": "string", "nullable": True},
    #             {"name": "event_type", "type": "string", "nullable": True},
    #             {"name": "blob_url", "type": "string", "nullable": True},
    #             {"name": "workflow_id", "type": "string", "nullable": False},
    #             {"name": "run_status", "type": "string", "nullable": True},
    #             {"name": "run_attempts", "type": "long", "nullable": False}
    #             ])

    def get_servicebus_receiver(self):
        """ Construct the service bus receiver """
        with self.sb_client as client:
            receiver = client.get_queue_receiver(queue_name=self.queue_name)
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
                            self.process_message(message)
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

    def check_kv_for_message(self, message_id):
        """ Check to see if an entry with the specified Azure Service Bus"""
        logging.info("is message in kv store?")
        try:
            df = self.v3io_client.read(backend="nosql", table=self.table)
        except v3f.errors.WriteError:
            return False, None
        except Exception as e:
            raise RuntimeError(
                f"Failed to complete check for message" f"in kv with {e}"
            )
        if message_id in df.index.tolist():
            logging.info("message_id is in kv store")
            df = df[df.index == message_id]
            return True, df
        else:
            return False, None

    def update_kv_data(self, message, action=None):
        """ Add the Service Bus message to the kv table"""
        try:
            if action in ["update_entry", "delete_entry"]:
                # Remove the entry
                filter = f"message_id=='{message.get('message_id')}'"
                self.v3io_client.delete(backend="kv", table=self.table, filter=filter)
            if action == "delete_entry":
                return
            logging.info("Adding message to kv...")
            df = pd.DataFrame.from_dict(data=message, orient="index").T
            logging.info("Message converted to DataFrame")
            df = df.set_index("message_id")
            self.v3io_client.write(backend="nosql", table=self.table, dfs=df)
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

    def get_run_status(self, workflow_id):
        """
        Retrieves the status of a pipeline run from the mlrun database

        :param workflow_id: A workflow_id from the mlrun database
        :return The run status of a pipeline
        """

        db = get_run_db().connect()
        pipeline_info = db.get_pipeline(run_id=workflow_id)
        return pipeline_info.get("run").get("status")

    def process_message(self, message):
        """
        Write the logic here to parse the incoming message.

        :param message: A Python dict of the message received from
            Azure Service Bus
        """
        logging.info("Process the incoming message")
        message_id = message.get("id", None)
        if message_id is None:
            raise ValueError("Unable to identify message_id!")
        message_topic = message.get("topic", None)
        event_type = message.get("eventType", None)
        data = message.get("data", None)
        if data is not None:
            blob_url = data.get("blobUrl", None)
            # Reformat the blob_url to a fsspec-compatible file location
            abfs_account, abfs_path = self._parse_blob_url_to_fsspec_path(blob_url)

        processed_message = {
            "message_id": message_id,  # The messageId from Service Bus
            "message_topic": message_topic,  # messageTopic from Service Bus
            "event_type": event_type,  # The eventType from Service Bus
            "blob_url": blob_url,  # The blobUrl -- The blob created
            "workflow_id": None,  # This is the workflow_id set by mlrun
            "run_status": None,  # This is the run_status in Iguazio
            #                             "run_attempts": 0,
            # We will zero-index count the number of run attempts for
            # a given run
            "abfs_account_name": abfs_account or None,
            "abfs_path": abfs_path or None,
        }

        # Check to see if the message_id is in the nosql run table
        has_message, df = self.check_kv_for_message(message_id)
        if not has_message:
            # If the message_id is not in the k,v store, Add it
            # to the store and run the pipeline, returning the
            # workflow_id and the run_status
            logging.info("message_id not found in kv store")
            self.update_kv_data(processed_message)
            workflow_id = self.run_pipeline(event=processed_message)
            run_status = "Started"
            logging.info(f"workflow_id is:  {workflow_id}")
            if workflow_id is not None:
                # Here we're starting the pipeline and adding the workflow_id
                # to the kv store
                logging.info(f"workflow_id is:  {workflow_id}")
                processed_message["workflow_id"] = workflow_id
                processed_message["run_status"] = run_status
                self.update_kv_data(processed_message, action="update_entry")
        else:
            logging.info(
                "Found message_id in the kv store.  Check to see if the "
                "pipeline is running or has run"
            )
            run_status = df.squeeze()["run_status"]
            if run_status in [None, "Failed"]:
                workflow_id = self.run_pipeline(event=processed_message)
                run_status = "Started"
                processed_message["workflow_id"] = workflow_id
                processed_message["run_status"] = run_status
                self.update_kv_data(processed_message, action="update_entry")
            elif run_status == "Running":
                pass
            else:
                logging.info(f"run_status unknown:  {run_status}")

    def check_and_update_run_status(self):
        """
        This will be run in the Nuclio handler on a CRON trigger.
        We will retrieve a list of active runs from the KV store and
        check their status.  We can update the kv store if a run is done.
        """

        # Find any entries in the kv store with no status or in a running state
        logging.info("Set counter to zero")
        counter = 0
        query = "run_status in ('Started', 'Running')"

        while counter < 3:
            try:
                df = self.v3io_client.read(
                    backend="nosql", table=self.table, filter=query
                )
                if len(df.index) > 0:
                    # If there are runs in the kv store that are in a started
                    # or running state, check their run_status
                    # Handle retry logic where a run can be attempted up to 3
                    # times on the same day.

                    for workflow_id in df["workflow_id"].tolist():
                        logging.info(
                            f"Checking run info for workflow_id:" f"{workflow_id}"
                        )
                        current_run_info = (
                            df[df["workflow_id"] == workflow_id]
                            .reset_index()
                            .to_dict("records")[0]
                        )

                        run_status = self.get_ppeline_status(workflow_id)
                        if run_status == "Succeeded":
                            logging.info("run_status is 'Succeeded'")
                            current_run_info["run_status"] = run_status
                            self.update_kv_data(current_run_info, action="update_entry")
                        elif (
                            run_status == "Failed"
                        ):  # and current_run_info["run_attempts"] < 3:
                            logging.info(
                                "Run status in a failed state."
                                "Need to implement logic to retry!"
                            )
                            #   current_run_info["run_attempts"] += 1
                            #   workflow_id = self.run_pipeline
                            # (current_run_info)
                            #    run_status = self.get_run_status(workflow_id)
                            #   current_run_info["workflow_id"] = workflow_id
                            #   current_run_info["run_status"] = run_status
                            # self.update_kv_data(current_run_info, action =
                            # "update_entry")
                            pass
                        elif run_status in ["Started", "Running"]:
                            logging.info("Run is in progress...")
                            pass
                        elif run_status is None:
                            logging.info("run_status has a None state.")
                        else:
                            logging.info(
                                f"run_status is not known."
                                f"Got a run_status of {run_status}"
                            )
                else:
                    logging.info(
                        "No runs found in kv store that match"
                        "'Started' or 'Running' state."
                    )
            except Exception as e:
                logging.info(f"Caught exception {e}." "Update counter retry!")
                counter += 1
                time.sleep(10)
                continue
            break
