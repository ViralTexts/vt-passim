#!/usr/bin/env python

import json, re, sys

def main(argv):
    for line in sys.stdin:
        rec = json.loads(line)
        series = rec['Publication']['PublicationID']
        date = rec['NumericPubDate']
        date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
        print json.dumps({'id': 'aps/' + rec['RecordID'],
                          'issue': 'aps/' + series + '/' + date,
                          'series': 'aps/' + series,
                          'date': date,
                          'url': rec['URLFullText'],
                          'title': rec['RecordTitle'],
                          'category': rec['ObjectType'],
                          'text': rec['FullText']})
        # 'text': rec['FullText']}
                         # 'text': re.sub("\n$", "", re.sub("^\n+", "", rec['FullText']))}
        #)
                   

if __name__ == "__main__":
    main(sys.argv[1:])
