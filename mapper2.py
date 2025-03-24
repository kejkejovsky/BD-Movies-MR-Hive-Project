#!/usr/bin/env python

import sys

for line in sys.stdin:
	line = line.strip()
	words = line.split(",")
	
	if words[1] == "" or words[1] == "film_id":
		continue
	
	if words[3] == "" or words[3] == "rate":
		continue
		
	print("%s\t%d,%s" %(words[1], 1, words[3]))