#!/usr/bin/env python
"""
Script to fetch DBpedia data
"""

import sys, time
from urllib.request import urlopen
from urllib.parse import unquote
import json

def main():
    for line in sys.stdin.readlines():
        line = line.strip()
        norm = unquote(line)
        url = line.replace('/resource/', '/data/') + '.json'
        time.sleep(1)
        # print(url)
        try:
            resp = urlopen(url)
            if resp.code == 200:
                data = json.loads(resp.read())[norm]
                print(json.dumps({'coverage': line,
                                  'lon': data['http://www.w3.org/2003/01/geo/wgs84_pos#long'][0]['value'],
                                  'lat': data['http://www.w3.org/2003/01/geo/wgs84_pos#lat'][0]['value']},
                                 sort_keys=True))
        except:
            print(json.dumps({'coverage': line}))
            False

if __name__ == "__main__":
    main()
