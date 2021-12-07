
def isKeyPresent(obj,key):
    return key in obj

def getKeyValue(obj,key):
    if isKeyPresent(obj, key):
        return obj[key]
    return None