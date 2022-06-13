"""
Script to copy data from one elastic search into another.

Set the environment parameters:
ES_URL_FROM -> Elastic search from which to carry
ES_INDEX_FROM -> Index from which we need to copy the data
ES_URL_TO -> Elastic search to which to carry
ES_INDEX_TO -> Index to which we need to copy the data
"""


import json
from collections import defaultdict
from structure import ElasticStructure
import constants as constant
import requests

from utility import uploadData

# Environment file
from dotenv import load_dotenv
import os
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
# -------------------------------------------------------------------------------------------

ES_URL_FROM = os.environ.get("ES_URL_FROM")
ES_INDEX_FROM = os.environ.get("ES_INDEX_FROM")
ES_URL_TO = os.environ.get("ES_URL_TO")
ES_INDEX_TO = os.environ.get("ES_INDEX_TO")

retries = 3


# Fetch records
base_url = ES_URL_FROM+ES_INDEX_FROM
data_present_in_es = []
r = requests.get(base_url+"/_count")
data = r.json()
print(base_url)
total_records = data['count']

from_no = 0
batch_size = 10000
to_no = batch_size


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


# ------------------------------------------------------------------------------------------------------------------------------
# LETS DO BATCH PROCSSING
# COPY A BATCH OF DATA AND PUBLISH IT TO THE TO ES

while True:
    print("Round starts......")
    url = base_url+"/_search?size="+str(batch_size)+"&from="+str(from_no)
    from_no += batch_size
    r = requests.get(url)
    data = r.json()
    print(url)
    res = []
    if data and data.get('hits', False):
        data = data['hits']['hits']
        for obj in data:
            res.append(obj['_source'])
        processDataUpload(res, 0)

    else:
        break
    if from_no > total_records:
        break
