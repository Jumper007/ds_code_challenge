# Remove SubCouncil2016, Wards2016, OfficialSuburbs, Code and h3_level8_index
# -*- coding: utf-8 -*-
"""
Created on 17 June 2021

Created by J Botha

This script attempts to anonymises the sr_hex.csv file

location accuracy to within approximately 500m (Type Error with NaN records, replaced NaN values with Zeroes 
but it seems to have messed up the data in those cols).
temporal accuracy to within 6 hours (still have to look at this)
scrubs any columns which may contain personally identifiable information. (Done on cols ["SubCouncil2016", "Wards2016", "OfficialSuburbs", "h3_level8_index"])

How to use:
Modules Needed:
-pip install pandas

Files needed:
Input file: "sr_hex_truncated.csv" file that has been provided should be in the same directory.
Output file: "anonymised_sr_hex_truncated.csv" file gets generated by this application.

Run: python further_data_transformations.py
"""

import pandas as pd
import datetime
import os


# Tracking Time taken for application to run
application_start_time = datetime.datetime.now()

# fuction scrubbing data cols that needs to be anonymised.
def clean_df(df, cols):
	for col_name in cols:
		keys = {cats: i for i, cats in enumerate(df[col_name].unique())}
		df[col_name] = df[col_name].apply(lambda x: keys[x])
	return df
	
# function setting location accuracy to within approximately 500m
def location_df(df, l_cols):
	for col_name in l_cols:
		keys = {cats: i for i, cats in enumerate(df[col_name].unique())}
		df[col_name] = df[col_name].apply(lambda x: keys[x]+0.00400)			
				
	return df
	
# looping in current directory to find input file
for each_file in sorted(os.listdir('.')):
	#open input file provided
	if each_file.endswith("sr_hex_truncated.csv"):
		
		df = pd.read_csv(each_file)
		# replace NaN values by Zeroes
		df = df.fillna(0)
		# Cols we scrubbing 
		cols = ["SubCouncil2016", "Wards2016", "OfficialSuburbs", "h3_level8_index"]		
		df = clean_df(df, cols)
		# location accuracy to within approximately 500m
		l_cols = ["Latitude", "Longitude"]
		df = location_df(df, l_cols)
		
		#Output file being created
		df.to_csv("anonymised_sr_hex_truncated.csv")
	
application_end_time = datetime.datetime.now()
application_time_taken = application_end_time - application_start_time

# Process time stats
print("application_start_time = ", application_start_time)
print("application_end_time = ", application_end_time)
print("application_time_taken = ", application_time_taken)