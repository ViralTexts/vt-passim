#!/usr/bin/env python
"""
Script to fetch DBpedia-Wikidata links
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
        time.sleep(0.1)
        # print(url)
        try:
            resp = urlopen(url)
            if resp.code == 200:
                data = json.loads(resp.read())[norm]
                links = [r['value'].replace('http://www.wikidata.org/entity/', '', 1)
                         for r in data['http://www.w3.org/2002/07/owl#sameAs']
                         if r['type'] == 'uri'
                         and r['value'].startswith('http://www.wikidata.org/entity/')]
                links.sort(key=lambda s: int(s.replace('Q', '', 1)))
                print(json.dumps({'coverage': line,
                                  'wdid': links[0]}))
        except:
            print(json.dumps({'coverage': line}))
            False

if __name__ == "__main__":
    main()
