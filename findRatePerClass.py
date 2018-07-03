#!/usr/bin/env python
"""
Find mutation rate per repeat class.
"""
from argparse import ArgumentParser
from collections import defaultdict

def main():
    parser = ArgumentParser()
    parser.add_argument('liftoverPsl')
    opts = parser.parse_args()

    totalSizeByClass = defaultdict(int)
    totalMismatchesByClass = defaultdict(int)
    ratesByClass = defaultdict(list)

    with open(opts.liftoverPsl) as f:
        for line in f:
            fields = line.split()
            assert len(fields) == 22, "Invalid number of fields in PSL-with-name input"
            name = fields[0]
            mismatches = int(fields[2])
            total = int(fields[1]) + int(fields[2]) + int(fields[3])
            if total == 0:
                # Avoid division by zero
                continue
            totalSizeByClass[name] += total
            totalMismatchesByClass[name] += mismatches
            ratesByClass[name] += [float(mismatches)/total]

    overallRateByClass = dict((name, float(mm)/totalSizeByClass[name]) for name, mm in totalMismatchesByClass.items())
    for k,v in overallRateByClass.items():
        print k, v, totalSizeByClass[k]

    print float(sum(totalMismatchesByClass.values()))/sum(totalSizeByClass.values()), sum(totalSizeByClass.values())

if __name__ == '__main__':
    main()
