import json
from collections import defaultdict
from utility import isKeyPresent, getKeyValue,dataExtractionMapper
from structure import ElasticStructure
import constants as constant


### Environment file
from dotenv import load_dotenv
import os
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
## -------------------------------------------------------------------------------------------

## Configurations
config_file = open("config.json")
json_data=json.load(json_file)

