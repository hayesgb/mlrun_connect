# mlrun_connect

A collection of tools to simplify integration between MLRun and services from cloud providers.


# Quickstart

The package can be installed using:
`pip install mlrun_connect`

MLRun is an open-source MLOps orchestration framework.  It enables end-to-end development of
machine learning models, from exploratory data analysis to prototyping to operationalization.

A common use case would be to install MLRun on-premise or with a cloud provider, and connect
to data sources for exploratory analysis.  While the Nuclio library offers a HTTP-based approach
to integration with external services, there are a variety of other approaches that may be prefered
(i.e. messaging systems).

mlrun_connect will provide tools to ease integration with these services.

# Azure Service Bus Queue

The AzureSBTMLRun class can be in conjunction with a Nuclio function to initiate the execution of a mlrun pipeline
based on an incoming message.  The AzureSBToMLRun object becomes the parent to a new class that is instantiated
within the Nuclio init_context function, as follows:

```
from mlrun_connect.azure import AzureSBToMlrun

def init_context(context):
    pipeline = load_project(<PATH_TO_MLRUN_PROJECT>)
    class SBHandler(AzureSBToMLRun):
        def run_pipeline(self, event):
            arguments = {"incoming_data": event["key"]}
            workflow_id = pipeline.run(arguments = arguments)
            return workflow_id
            
```