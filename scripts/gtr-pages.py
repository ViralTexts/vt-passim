from __future__ import print_function
import json, os, sys

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: gtr-pages.py <input directory> <output directory>', file=sys.stderr)
        exit(1)
    indir = sys.argv[1]
    outdir = sys.argv[2]
    os.mkdir(outdir)
    for file in os.listdir(os.fsencode(indir)):
        fname = os.fsdecode(file)
        if fname.endswith('.json'):
            for line in open(os.path.join(indir, fname), 'r'):
                rec = json.loads(line)
                out = open(os.path.join(outdir, rec['name'] + '.html'), 'w')
                print(rec['text'], file=out)
                out.close()
