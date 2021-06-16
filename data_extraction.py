from aws_credentials import access_key, secret_key, aws_region
from boto3.session import Session

import boto3

# Compare new data set vs existing data set
new_data_list = []
base_data_list = []

# create boto session
session = Session(
	aws_access_key_id="AKIAYH57YDEWMHW2ESH2",
	aws_secret_access_key=secret_key,
	region_name=aws_region
	)

# make connection
client = session.client('s3')

# getting data from city-hex-polygons-8-10.geojson
resp = client.select_object_content(
	Bucket = "cct-ds-code-challenge-input-data",
	Key = "city-hex-polygons-8-10.geojson",
	Expression = "SELECT d.type, d.properties, d.geometry FROM  S3Object[*].features[*] d WHERE d.properties.resolution = 8",
	ExpressionType = "SQL",
	InputSerialization = {"JSON": {"Type": "DOCUMENT"}},
	OutputSerialization = {"JSON": {"RecordDelimiter": ", "}}
)

# getting data from city-hex-polygons-8.geojson
base_resp_standard = client.select_object_content(
				Bucket = "cct-ds-code-challenge-input-data",
				Key = "city-hex-polygons-8.geojson",
				Expression = "SELECT d.type, d.properties, d.geometry FROM  S3Object[*].features[*] d",
				ExpressionType = "SQL",
				InputSerialization = {"JSON": {"Type": "DOCUMENT"}},
				OutputSerialization = {"JSON": {"RecordDelimiter": ", "}}
)


# Loop through payload objects then comparing it to make sure the data is the same
for event in resp["Payload"]:
	if "Records" in event:
		new_data_list.append(event["Records"]["Payload"].decode())
		
for event in base_resp_standard["Payload"]:
	if "Records" in event:
		base_data_list.append(event["Records"]["Payload"].decode())

if new_data_list.sort() == base_data_list.sort():
	print("city-hex-polygons-8-10.geojson dataset matches city-hex-polygons-8.geojson dataset as requested")
else:
	print("city-hex-polygons-8-10.geojson dataset does not match city-hex-polygons-8.geojson dataset as requested")
	
	