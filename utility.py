
def isKeyPresent(obj,key):
    return key in obj

def getKeyValue(obj,key):
    if isKeyPresent(obj, key):
        return obj[key]
    return None


def dataExtractionMapper(obj,keys):
    temp=dict()
    for key in  keys:
        temp[key]=getKeyValue(obj, key)
    return temp
    
