import json
from collections import defaultdict
from random import randint
import requests
from datetime import date, timedelta, datetime
from utility import uploadData
from copy import deepcopy
# Environment file
from dotenv import load_dotenv
import os
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
# -------------------------------------------------------------------------------------------

ES_URL = os.environ.get("ES_URL")
ES_INDEX_WRITE = os.environ.get("METRICS_INDEX")
ES_INDEX_READ = os.environ.get("METRICS_INDEX_DEV")


# Fetch records
base_url = ES_URL+ES_INDEX_READ
data_present_in_es = []
r = requests.get(base_url+"/_count")
data = r.json()
print(base_url)
print(data)
total_records = data['count']

from_no = 0
batch_size = 10000
to_no = batch_size

# Form the dates
current_date = date.today().isoformat()
dates = []
d = datetime.now()
no_of_days = 91
for i in range(0, no_of_days):
    last_date = d - timedelta(days=i)
    t = str(last_date.strftime("%Y-%m-%dT%H:%M:%S.%f"))
    dates.append(t)

dates.reverse()

print(dates)


def formObject(obj):
    obj['account_id'] = 'b'+str(obj['account_id'])
    obj['advertiser_id'] = 445
    # temp = []
    print("Processing........")
    for d in dates:
        updt = deepcopy(obj)
        r = randint(0, 100)
        updt['date'] = d
        updt['impressions'] = 1000*r
        updt['clicks'] = 10*r+10
        updt['localSpend'] = 15*r
        updt['spend'] = 15*r

        data_present_in_es.append(updt)
    # return temp


res_dict = {}

while True:
    url = base_url+"/_search?size="+str(batch_size)+"&from="+str(from_no)
    from_no += batch_size
    r = requests.get(url)
    data = r.json()
    print(url)
    if data and data.get('hits', False):
        data = data['hits']['hits']
        for obj in data:
            # print(obj)
            updt_obj = obj['_source']
            res_dict[updt_obj['creative_id']] = updt_obj
    else:
        break
    print("Round starts......")
    if from_no > total_records:
        break
# print(data_present_in_es)

for key, item in res_dict.items():
    # data_present_in_es.extend(formObject(item))
    formObject(item)


try:
    with open('response.json', 'w') as f:
        json.dump(data_present_in_es, f)
    f.close()
    print("Succesfully completed the process")
except:
    print("Error occurred while saving the data into json")

print(total_records)
print(len(data_present_in_es))

# Post data to elasticsearchj

# print(data_present_in_es)

post_url = ES_URL+ES_INDEX_WRITE+"/_bulk"
try:
    uploadData(data_present_in_es, post_url, batch_size)
    print("Succesfully updated data")
except:
    print("Error occurred while uploading the data")
