from aws_credentials import access_key, secret_key, aws_region
from boto3.session import Session
from io import StringIO
from csv import reader

import boto3
import pandas as pd
import os
import itertools
import csv

list_of_rows = []


for each_file in sorted(os.listdir('.')):
	#open input file provided
	if each_file.endswith("sr.csv"):
		 #read csv file as a list of lists
		  with open(each_file, 'r') as read_obj:
		        # pass the file object to reader() to get the reader object
		        csv_reader = reader(read_obj)
		        # reading in the first 10 000 records as a sample set
		        for row in itertools.islice(csv_reader, 10000):
		        # Pass reader object to list() to get a list of lists
		            list_of_rows.append(row)

# create boto session
session = Session(
	aws_access_key_id="AKIAYH57YDEWMHW2ESH2",
	aws_secret_access_key=secret_key,
	region_name=aws_region
	)

# make connection
client = session.client('s3')

#  query and create response
base_resp_standard = client.select_object_content(
				Bucket = "cct-ds-code-challenge-input-data",
				Key = "city-hex-polygons-8.geojson",
				Expression = "SELECT d.properties FROM  S3Object[*].features[*] d",
				ExpressionType = "SQL",
				InputSerialization = {"JSON": {"Type": "DOCUMENT"}},
				OutputSerialization = {"JSON": {'RecordDelimiter': "\n"}}
)

#  upack query response
records = []
enhanced_list = []

for event in base_resp_standard["Payload"]:
	if "Records" in event:
		records.append(event["Records"]["Payload"])
		
#  store unpacked data as a CSV format
file_str = ''.join(req.decode('utf-8') for req in records)
    
#  read CSV to dataframe
df = pd.read_csv(StringIO(file_str))

for index, row in df.iterrows():
	tmp_list = []
	# h3_level8_index
	tmp_list.append(row[0].split(":")[2].strip('"'))
	# db_latitude
	tmp_list.append(row[1].split(":")[1])
	# db_longitude
	tmp_list.append(row[2].split(":")[1].split("}")[0])
	enhanced_list.append(tmp_list)

# open output file
with open('sr_updated.csv', 'w', encoding='UTF8', newline='') as f:
		writer = csv.writer(f)
		header = ['', 'NotificationNumber', 'NotificationType', 'CreationDate', 'CompletionDate', 'Duration', 'CodeGroup', 'Code', 'Open', 'Latitude', 'Longitude', 'SubCouncil2016', 'Wards2016', 'OfficialSuburbs', 'directorate', 'department', 'ModificationTimestamp', 'CompletionTimestamp', 'CreationTimestamp', 'h3_level8_index']

		# write the header to output file
		writer.writerow(header)
		
		# Loop through input data set and 
		for row1 in list_of_rows:
			if row1[10] == 'nan':
				existing_row = row1
				existing_row.append(0)
				writer.writerow(existing_row)
			for row2 in enhanced_list:
				if row1[10] == row2[2] and row1[9] == row2[1]:
					enhanced_row = row1.append(row2[0])
					writer.writerow(enhanced_row)