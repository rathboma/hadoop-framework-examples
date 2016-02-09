#!/usr/bin/env python
import sys
import string

last_user_id = None
cur_location = "-"

for line in sys.stdin:
    line = line.strip()
    user_id,product_id,location = line.split("\t")
    # if this is the first iteration
    if not last_user_id or last_user_id != user_id:
        last_user_id = user_id
        cur_location = location
    elif user_id == last_user_id:
        location = cur_location
        print '%s\t%s' % (product_id,location)


