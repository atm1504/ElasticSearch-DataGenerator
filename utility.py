import random
import pandas
from datetime import datetime


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
