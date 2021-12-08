import json
from collections import defaultdict
from utility import isKeyPresent, getKeyValue, dataExtractionMapper, getRandomInt, getRandomFloat
from structure import ElasticStructure
import constants as constant


# Environment file
from dotenv import load_dotenv
import os
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
# -------------------------------------------------------------------------------------------

# Configurations
config_file = open("config.json")
config = json.load(config_file)


""" 
Extract features from the data file
"""
input_file = open(config[constant.INPUT_FILE_PATH])
input_data = json.load(input_file)
output_type_file = open(config[constant.OUTPUT_TYPE_FILE_PATH])
output_type_data = json.load(output_type_file)


def parseIncomingData(json_data, keys_to_be_extracted):
    li = defaultdict(list)
    for data in json_data:
        # Extracting data
        source = getKeyValue(data, constant.SOURCE_)
        if not source:
            continue
        channel = getKeyValue(source,  constant.CHANNEL)

        li[channel].append(dataExtractionMapper(source, keys_to_be_extracted))
    return li


keys_to_be_extracted = config[constant.KEYS_TO_BE_EXTRACTED]
channel_wise_data = parseIncomingData(input_data, keys_to_be_extracted)


def getElasticObject(sample, low_int, high_int, low_float, high_float, parentObj, date, random_floats, random_integers):
    keys = sample.keys()
    print(keys)
    obj = dict()
    for key in keys:
        if key == constant.DATE:
            obj[key] = date
        elif key in parentObj:
            obj[key] = parentObj[key]
        elif key in random_floats:
            obj[key] = getRandomFloat(low_float, high_float)
        elif key in random_integers:
            obj[key] = getRandomInt(low_int, high_int)
        else:
            obj[key] = sample[key]

    return json.dumps(obj, indent=4)


random_floats = []
random_integers = []
if config and config[constant.RANDOM_INTEGERS]:
    random_integers = config[constant.RANDOM_INTEGERS]


if config and config[constant.RANDOM_FLOATS]:
    random_floats = config[constant.RANDOM_FLOATS]

print(getElasticObject(output_type_data, 0,
      1000,  0, 100, channel_wise_data["GOOGLE"][0], "2021-12-01T00:00:00.000Z", random_floats, random_integers))
