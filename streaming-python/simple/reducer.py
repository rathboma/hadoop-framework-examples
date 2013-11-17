#! /usr/bin/env python

import sys

last_turf = None
turf_count = 0

# Example input (notice how they're IN ORDER):
# FALSE 1
# FALSE 1
# FALSE 1
# FALSE 1
# TRUE 1
# TRUE 1
# UNKNOWN 1
# UNKNOWN 1

# keys come grouped together
# so we need to keep track of state a little bit
# thus when the key changes (turf), we need to reset
# our counter, and write out the count we've accumulated

for line in sys.stdin:

    line = line.strip()
    turf, count = line.split("\t")

    count = int(count)
    # if this is the first iteration
    if not last_turf:
        last_turf = turf

    # if they're the same, log it
    if turf == last_turf:
        turf_count += count
    else:
        # 
        result = [last_turf, turf_count]
        print("\t".join(str(v) for v in result))
        last_turf = turf
        turf_count = 1

# this is to catch the final value that we output
print("\t".join(str(v) for v in [last_turf, turf_count]))