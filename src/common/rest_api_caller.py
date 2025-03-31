import requests
import json

def get(url):
    headers = {
        'Content-Type': "application/json",
        'User-Agent': "apache spark 3.x"
    }
    res = None
    try:
        res = requests.get(url, headers=headers)
    except Exception as e:
        return e
    if res != None and res.status_code == 200:
        return json.loads(res.text)
    return None
