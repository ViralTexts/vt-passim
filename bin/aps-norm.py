#!/usr/bin/env python

import json, re, sys

def main(argv):
    for line in sys.stdin:
        rec = json.loads(line)
        if rec.has_key('Publication') and rec['Publication'].has_key('PublicationID'):
            series = rec['Publication']['PublicationID']
            date = rec['NumericPubDate']
            date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
            out = {'id': 'aps/' + rec['RecordID'],
                   'issue': 'aps/' + series + '/' + date,
                   'series': 'aps/' + series,
                   'date': date,
                   'lang': (rec['LanguageCode'].lower() if rec.has_key('LanguageCode') else 'eng'),
                   'url': (rec['URLFullText'] if rec.has_key('URLFullText') else ''),
                   'title': rec['RecordTitle'],
                   'category': rec['ObjectType'],
                   'text': (rec['FullText'] if rec.has_key('FullText') else '')}
            if rec.has_key('Contributor') and rec['Contributor'].has_key('OriginalForm'):
                out.update({'contributor': rec['Contributor']['OriginalForm']})
            print json.dumps(out)
            # 'text': rec['FullText']}
            # 'text': re.sub("\n$", "", re.sub("^\n+", "", rec['FullText']))}
            #)
                   

if __name__ == "__main__":
    main(sys.argv[1:])
