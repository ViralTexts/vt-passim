#!/usr/bin/env python
"""
Script to canonicalize DBPedia URLs wrt redirects.
"""

import sys
from urllib.request import urlopen
import json

def main():
    for line in sys.stdin.readlines():
        line = line.strip()
        try:
            resp = urlopen(line)
            if resp.code == 200:
                canon = resp.geturl().replace('/page/', '/resource/').replace(',', '%2C')
                if line != canon:
                    print(json.dumps({'url': line, 'canonical': canon}))
        except:
            print(json.dumps({'url': line, 'canonical': ''}))
            False

if __name__ == "__main__":
    main()
