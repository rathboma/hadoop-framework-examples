#!/usr/bin/env python
import sys
for line in sys.stdin:
	user_id = ""
	product_id = "-"
	location = "-"
	line = line.strip()         
	splits = line.split("\t")         
	if len(splits) == 5:
		user_id = splits[2]
		product_id = splits[1]
	else:
		user_id = splits[0]
		location = splits[3]                   
	print '%s\t%s\t%s' % (user_id,product_id,location)
