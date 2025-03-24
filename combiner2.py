#!/usr/bin/env python

import sys

last_key = None
sum_count = 0
sum_rate = 0

for input_line in sys.stdin:
	input_line = input_line.strip()
	key, value = input_line.split("\t", 1)
	value = value.split(",")
	count = int(value[0])
	rate = int(value[1])
	
	if last_key == key:
		sum_count += count
		sum_rate += rate
	else:
		if last_key:
			print("%s\t%d,%d" % (last_key, sum_count, sum_rate))
		sum_count = count
		sum_rate = rate
		last_key = key

if last_key == key:
	print("%s\t%d,%d" % (last_key, sum_count, sum_rate))