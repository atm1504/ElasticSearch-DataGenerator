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
#   date : "2022-06-22",
#   audienceId: "12345",
#   audienceExtId : "ext12345",
#   campaignId: "c12312",
#   campaignName: "campaign 1",
#   lineId: "l4432",
#   channel: "GOOGLE",
#   advertiserId: "23423",
#   advertiserName: "Advertiser 1",
#   totalImpressions : 773423,
#   cpm: 2.6,
#   totalCost:2010.8998,
#   dpCost: {
#     100: 200.0,
#     200: 500.00,
#     301: 1310.8998
#   },
#   uniqueSegments: [25,63,33,21,342,1,2,23,4,5,6],
#   totalSegmentsCount: 14,
#   segmentExp : <expression object>,
#   dpSegmentsCount: {
#     100: 4,
#     200: 2,
#     301: 8
#   },
#   dpUniqueSegments: {
#     100: [25,63,33],
#     200: [21,342],
#     301: [1,2,23,4,5,6]
#   }
# }

AUDIENCE_IDS = getRandomNumbers(10001, 96789, 10)
print(AUDIENCE_IDS)
DATES = getDates("2022-05-10", "2022-07-12")
CAMPAIGNS_ID = ['c12312', 'c13221']
DATAPARTNER_ID = getRandomNumbers(1111, 9999, 30)
CHANNELS = ['GOOGLE', 'FACEBOOK', 'YAHOOJP', 'TIKTOK', 'TWITTER']
SEGMENT_IDS = getRandomNumbers(10, 999, 120)

DATA_PARTNER_SEGMENT_MAP = {}

# Mapping between datapartner id an segments
i = 0
for x in DATAPARTNER_ID:
    t = randint(3, 4)
    segments = []
    for j in range(i+t):
        segments.append(SEGMENT_IDS[j])
    DATA_PARTNER_SEGMENT_MAP[x] = segments
    i += t

AUDIENCE_ID_DATA_PARTNER_MAP = {}
i = 0
# map between audience ids and datapartner ids
for x in AUDIENCE_IDS:
    dps = []
    for j in range(i+3):
        dps.append(DATAPARTNER_ID[j])
    AUDIENCE_ID_DATA_PARTNER_MAP[x] = dps
    i += 3

res = []

for date in DATES:
    for audienceId in AUDIENCE_IDS:
        for campaignId in CAMPAIGNS_ID:
            obj = dict()
            obj['date'] = date
            obj['audienceId'] = audienceId
            obj['audienceExtId'] = 'ext'+str(audienceId)
            obj['campaignId'] = campaignId
            obj['campaignName'] = 'campaign '+campaignId
            obj['channel'] = CHANNELS[randint(0, 4)]
            obj['lineId'] = 'l4432'

            obj['advertiserId'] = '23423'
            obj['advertiserName'] = 'Advertiser ' + obj['advertiserId']
            obj['totalImpressions'] = randint(17342, 9773423)
            obj['cpm'] = getRandomFloat(1, 4)

            dataPartners = AUDIENCE_ID_DATA_PARTNER_MAP[audienceId]
            dpCost = {}
            totalCost = 0
            uniqueSegments = []
            dpSegmentsCount = {}
            dpUniqueSegments = {}
            for x in dataPartners:
                dpCost[x] = getRandomFloat(10, 10000)
                totalCost += dpCost[x]
                seg = DATA_PARTNER_SEGMENT_MAP[x]
                uniqueSegments.extend(seg)
                dpSegmentsCount[x] = len(seg)
                dpUniqueSegments[x] = seg
            obj['dpCost'] = dpCost
            obj['totalCost'] = totalCost
            obj['uniqueSegments'] = uniqueSegments
            obj['dpSegmentsCount'] = dpSegmentsCount
            obj['dpUniqueSegments'] = dpUniqueSegments
            obj['segmentExp']=''

            res.append(obj)

try:
    with open('data.json', 'a') as f:
        json.dump(res, f)
    f.close()
except:
    print("Error occurred while saving the data into json")

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
    processDataUpload(res, 3)
    with open('data.json', 'w') as f:
        json.dump(res, f)
    f.close()
    print("Succesfully completed the process")
except:
    print("Error occurred while saving the data into json")
