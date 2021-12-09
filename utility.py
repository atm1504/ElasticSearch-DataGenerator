import random
import pandas
from datetime import datetime
import requests
import json


def isKeyPresent(obj, key):
    return key in obj


def getKeyValue(obj, key):
    if isKeyPresent(obj, key):
        return obj[key]
    return None


def dataExtractionMapper(obj, keys):
    temp = dict()
    for key in keys:
        temp[key] = getKeyValue(obj, key)
    return temp


def getRandomInt(low, high):
    return random.randint(low, high)


def getRandomFloat(low, high):
    return random.uniform(low, high)


def getDates(startDate, endDate):
    temp = pandas.date_range(startDate, endDate, freq='d')
    res = [str(x) for x in temp]
    return res


def uploadData(data, url, batch_size):
    data1 = {"index": {}}
    n = len(data)
    headers = {'Content-Type': 'application/json'}
    for startInd in range(0, n, batch_size):
        pdata = []
        for j in range(startInd, min(startInd+batch_size, n)):
            pdata.append(json.dumps(data1))
            pdata.append(json.dumps(data[j]))
            d = '\n'.join(pdata)+'\n'
        try:
            r = requests.post(url, data=d, headers=headers)
            print(r.json())
        except:
            print("Error occurred while making thee requests")
            return False
