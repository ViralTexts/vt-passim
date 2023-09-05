#!/usr/bin/env python
"""
Script to canonicalize DBPedia URLs wrt redirects.
"""

import sys
from urllib.request import urlopen
import json

def main():
    # sys.stdin.reconfigure(encoding='utf-8')
    for line in sys.stdin.readlines():
        line = line.strip()
        rec = json.loads(line)
        try:
            resp = urlopen(rec['coverage'])
            if resp.code == 200:
                canon = resp.geturl().replace('/page/', '/resource/').replace(',', '%2C')
                rec['canon'] = canon
                print(json.dumps(rec, ensure_ascii=False))
                # if line != canon:
                #     print(json.dumps({'url': line, 'canonical': canon}))
        except:
            print(line)
            False

if __name__ == "__main__":
    main()
