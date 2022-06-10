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

ES_URL = os.environ.get("ES_URL")
ES_INDEX = os.environ.get("METRICS_INDEX")


# Fetch records
base_url = ES_URL+ES_INDEX+"/_search"
data_present_in_es = []
r = requests.get(base_url)
data = r.json()

total_records = data['hits']['total']['value']

from_no = 0
batch_size = 200
to_no = batch_size

while True:
    url = base_url+"?size="+str(batch_size)+"&from="+str(from_no)
    from_no += batch_size
    r = requests.get(url)
    data = r.json()
    data = data['hits']['hits']
    for obj in data:
        updt_obj = obj['_source']
        updt_obj['account_id'] = 'b'+str(updt_obj['account_id'])
        updt_obj['advertiser_id'] = 445
        data_present_in_es.append(updt_obj)

    if from_no > total_records:
        break


# Post data to elasticsearchj

print(data_present_in_es)

post_url = ES_URL+ES_INDEX+"/_bulk"
try:
    uploadData(data_present_in_es, post_url, 200)
    print("Succesfully updated data")
except:
    print("Error occurred while uploading the data")
