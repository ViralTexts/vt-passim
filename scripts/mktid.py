#!/usr/bin/env python
"""
Generate Title IDs
"""

from re import sub
import json, sys

def main():
    for line in sys.stdin.readlines():
        line = line.strip()
        rec = json.loads(line)
        if rec['series'] == None:
            rec['series'] = 'tid/' + \
                sub(r'[ ]+', '-',
                    sub(r'^((?:\S+\s+){,5}).*(\d{4})$', '\\1 \\2',
                        sub(r'^(the|a|an|la|le|il|el|die|der|das) ', '',
                            sub(r'[^A-Za-z0-9 ]', '',
                                sub(r'\s*\((\d{4})\-.*$', ' \\1',
                                    rec['title'].lower())))))
            print(json.dumps(rec))
        else:
            print(line)
        
if __name__ == '__main__':
    main()
