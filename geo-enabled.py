# find geo enabled tweets in file with json tweet content
import json
import sys

filename=sys.argv[1]
print "Filename: {0}".format(filename)
with open(filename) as data_file:
	count = 0 
	for line in data_file:
		data = json.loads(line)
		# if not data["user"]["geo_enabled"]:
		if data["geo"]:
			print "found one! id: {0}".format(data["id"])
			count = count + 1
print "Total geo_enabled: {0}".format(count)