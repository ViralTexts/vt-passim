#!/usr/bin/env python
"""
Script to fetch DBpedia-Wikidata links
"""

import sys, time
from wikidata.client import Client
import json

def main():
    client = Client()
    coord_prop = client.get('P625')
    country_prop = client.get('P17')
    litate_prop = client.get('P131') # located in the administrative territorial entity
    for line in sys.stdin.readlines():
        line = line.strip()
        try:
            indata = json.loads(line)
            id = indata['wdid']
            entity = client.get(id, load=True)
            coord = entity[coord_prop]
            hier = list()
            container = entity
            while litate_prop in container:
                container = container[litate_prop]
                # print(str(container.label))
                hier.append(str(container.label))
            country = str(entity[country_prop].label)
            topdiv = str(entity.label)
            if len(hier) == 1 and hier[-1] != country:
                topdiv = hier[-1]
            elif len(hier) > 1:
                if hier[-2] == country:
                    if len(hier) > 2:
                        topdiv = hier[-3]
                else:
                    topdiv = hier[-2]
                
            print(json.dumps({'coverage': indata['coverage'],
                              'wdid': indata['wdid'],
                              'label': str(entity.label),
                              'lon': coord.longitude,
                              'lat': coord.latitude,
                              'country': country,
                              'topdiv': topdiv,
                              'hier': hier}))
        except:
            print(line)
            False

if __name__ == '__main__':
    main()
