import json
from collections import defaultdict
from utility import isKeyPresent, getKeyValue
from constants import DATA_FILE


""" 
Extract features from the 
"""
json_file = open(DATA_FILE)
json_data=json.load(json_file)
# print(json_data[0])

channel_wise_data=defaultdict(list)

for data in json_data:
    ## Extracting data
    source=getKeyValue(data, "_source")
    if not source:
        continue
    channel = getKeyValue(source,  "channel")
    creative_id=getKeyValue(source, "creative_id")
    channel_wise_data[channel].append(creative_id)

print(channel_wise_data)