import json
from collections import defaultdict
from utility import isKeyPresent, getKeyValue
import constants as cnst
from dotenv import load_dotenv
import os
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Extract data from env file
data_file_name=os.environ.get(cnst.DATA_FILE_NAME)


""" 
Extract features from the data file
"""
json_file = open(data_file_name)
json_data=json.load(json_file)

channel_wise_data=defaultdict(list)

for data in json_data:
    ## Extracting data
    source=getKeyValue(data,cnst.SOURCE_)
    if not source:
        continue
    channel = getKeyValue(source,  cnst.CHANNEL)
    # creative_id=getKeyValue(source, cnst.CREATIVE_ID)

    temp=dict()
    temp[cnst.CREATIVE_ID]=getKeyValue(source, cnst.CREATIVE_ID)
    temp[cnst.AGENCY_ID]=getKeyValue(source, cnst.AGENCY_ID)
    temp[cnst.LINE_ID]=getKeyValue(source, cnst.LINE_ID)
    temp[cnst.ADVERTISER_ID]=getKeyValue(source, cnst.ADVERTISER_ID)
    temp[cnst.ACCOUNT_ID]=getKeyValue(source, cnst.ACCOUNT_ID)
    temp[cnst.CAMPAIGN_ID]=getKeyValue(source, cnst.CAMPAIGN_ID)


    channel_wise_data[channel].append(temp)

print(channel_wise_data)

