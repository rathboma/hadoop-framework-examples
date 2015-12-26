#!/usr/bin/env python
import sys
import string

for line in sys.stdin:
    line = line.strip()
    product_id,location = line.split("\t")
    print '%s\t%s' % (product_id,location)
