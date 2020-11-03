import boto3
import json
client = boto3.client('sagemaker-runtime')
custom_attributes = "c000b4f9-df62-4c85-a0bf-7c525f9104a4"  # An example of a trace ID.
endpoint_name = "jake-endpoint-test-2"                                       # Your endpoint name.
content_type = "application/json"                                        # The MIME type of the input data in the request body.
accept = "application/json"                                              # The desired MIME type of the inference in the response.
request_dict = json.dumps({"eta_requests":[{"store_id":2951,"delivery_zipcode":"21117","dow":"Fri", "hod":18}]})
payload = request_dict
response = client.invoke_endpoint(
    EndpointName=endpoint_name,
    CustomAttributes=custom_attributes,
    ContentType=content_type,
    Accept=accept,
    Body=payload
    )
print(response)
print(response['Body'].read())