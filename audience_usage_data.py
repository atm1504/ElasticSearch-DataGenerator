import json
from collections import defaultdict
from structure import ElasticStructure
import constants as constant
import requests
from random import randint

from utility import getDates, uploadData, getRandomNumbers, getRandomFloat

# Environment file
from dotenv import load_dotenv
import os
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
# -------------------------------------------------------------------------------------------


ES_URL_TO = os.environ.get("ES_URL_TO")
ES_INDEX_TO = os.environ.get("AUDIENCE_DATA_USAGE_INDEX")
batch_size = 10000
retries = 3

# {
# date : "2022-06-22",
# audienceId: "12345",
# audienceExtId : "ext12345",
# campaignId: "c12312",
# campaignName: "campaign 1",
# lineId: "l4432",
# uniqueSegments: [123,445,520],
# segmentExp : <expression object>,
# channel: "GOOGLE",
# advertiserId: "23423",
# advertiserName: "Advertiser 1",
# totalImpressions : 773423,
# cpm: 2.6,
# totalCost:2010.8998
# }

AUDIENCE_IDS = getRandomNumbers(10001, 96789, 10)
print(AUDIENCE_IDS)
DATES = getDates("2022-05-22", "2022-07-12")
CAMPAIGNS_ID = ['c12312', 'c13221']
CHANNELS = ['GOOGLE', 'FACEBOOK', 'YAHOOJP', 'TIKTOK', 'TWITTER']

# Populate
res = []

for date in DATES:
    for audienceId in AUDIENCE_IDS:
        for campaignId in CAMPAIGNS_ID:
            obj = dict()
            obj['date'] = date
            obj['audienceId'] = audienceId
            obj['campaignId'] = campaignId
            obj['campaignName'] = 'campaign '+campaignId
            obj['lineId'] = 'l4432'
            obj['uniqueSegments'] = getRandomNumbers(111, 999, randint(1, 13))
            obj['segmentExp'] = ''
            obj['channel'] = CHANNELS[randint(0, 4)]
            obj['advertiserId'] = '23423'
            obj['advertiserName'] = 'Advertiser ' + obj['advertiserId']
            obj['totalImpressions'] = randint(17342, 9773423)
            obj['cpm'] = getRandomFloat(1, 4)
            obj['totalCost'] = getRandomFloat(1900, 8767)
            res.append(obj)


def processDataUpload(data, tries):
    print("Processing data..........")
    post_url = ES_URL_TO+ES_INDEX_TO+"/_bulk"
    try:
        uploadData(data, post_url, batch_size)
        try:
            with open('response.json', 'a') as f:
                json.dump(data, f)
            f.close()
            print("Succesfully completed the process")
        except:
            print("Error occurred while saving the data into json")
        print("Succesfully updated data")
    except:
        print("Error occurred while uploading the data")
        if tries < retries:
            processDataUpload(data, tries+1)


try:
    processDataUpload(res,3)
    with open('data.json', 'w') as f:
        json.dump(res, f)
    f.close()
    print("Succesfully completed the process")
except:
    print("Error occurred while saving the data into json")
