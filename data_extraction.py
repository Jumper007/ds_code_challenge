# -*- coding: utf-8 -*-
"""
Created on 16 June 2021

Created by J Botha

Use the AWS S3 SELECT command to read in the H3 resolution 8 data from city-hex-polygons-8-10.geojson. 
Use the city-hex-polygons-8.geojson file to validate your work.

How to use:
Modules Needed:
-pip install boto3

Files needed:
"aws_credentials.py" file has been uploaded to the root directory.

Run: python data_extraction.py
"""
from aws_credentials import access_key, secret_key, aws_region
from boto3.session import Session

import boto3
import datetime

# Tracking Time taken for application to run
application_start_time = datetime.datetime.now()

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

# Making sure the data from the two datasets matches
if new_data_list.sort() == base_data_list.sort():
	print("city-hex-polygons-8-10.geojson dataset matches city-hex-polygons-8.geojson dataset as requested")
else:
	print("city-hex-polygons-8-10.geojson dataset does not match city-hex-polygons-8.geojson dataset as requested")

application_end_time = datetime.datetime.now()
application_time_taken = application_end_time - application_start_time

# Process time stats
print("application_start_time = ", application_start_time)
print("application_end_time = ", application_end_time)
print("application_time_taken = ", application_time_taken)
