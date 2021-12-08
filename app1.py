import json
from collections import defaultdict
from utility import isKeyPresent, getKeyValue,dataExtractionMapper
import constants as cnst
from dotenv import load_dotenv
import os
from os.path import join, dirname
from structure import ElasticStructure

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

keys_to_be_extracted=[cnst.CREATIVE_ID, cnst.CHANNEL, cnst.AGENCY_ID, cnst.LINE_ID, cnst.ADVERTISER_ID, cnst.ACCOUNT_ID, cnst.CAMPAIGN_ID]

def parseIncomingData():
    for data in json_data:
        ## Extracting data
        source=getKeyValue(data,cnst.SOURCE_)
        if not source:
            continue
        channel = getKeyValue(source,  cnst.CHANNEL)
        # creative_id=getKeyValue(source, cnst.CREATIVE_ID)

        # temp=dict()
        # temp[cnst.CREATIVE_ID]=getKeyValue(source, cnst.CREATIVE_ID)
        # temp[cnst.AGENCY_ID]=getKeyValue(source, cnst.AGENCY_ID)
        # temp[cnst.LINE_ID]=getKeyValue(source, cnst.LINE_ID)
        # temp[cnst.ADVERTISER_ID]=getKeyValue(source, cnst.ADVERTISER_ID)
        # temp[cnst.ACCOUNT_ID]=getKeyValue(source, cnst.ACCOUNT_ID)
        # temp[cnst.CAMPAIGN_ID]=getKeyValue(source, cnst.CAMPAIGN_ID)
        channel_wise_data[channel].append(dataExtractionMapper(source,keys_to_be_extracted))

parseIncomingData()

# print(channel_wise_data)

"""
Generate the  data in a particular structure
"""

def createObject(data):
    pass

    

for channel, datas in channel_wise_data.items():
    print(channel)
    for data in datas:
        print(data)
        # obj=ElasticStructure(date, conversions, ingestion_time, channel, agency_id, impressions, line_id, creative_id, video_view_complete, advertiser_id, account_id, job_id, spend, video_view_25p, clicks, video_view_75p, campaign_id, video_view_50p)