#!/usr/bin/env python
import json
import sys

for line in sys.stdin:
	line = line.strip()
	data = json.loads(line)
	# if not data["user"]["geo_enabled"]:
	if data["lang"]:
		print "{0}\t1".format(data["lang"])