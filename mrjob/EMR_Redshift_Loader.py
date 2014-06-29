#!/usr/bin/env python
from mrjob.job import MRJob
import json
from datetime import datetime
from mrjob.job import RawValueProtocol
import re

WORD_RE = re.compile(r"[\w']+")

class TweetToRedshift(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        try:
            line = line.strip()
            jsonData = json.loads(line)
            # if not data["user"]["geo_enabled"]:
            tweetLang=jsonData["lang"]
            tweetId=jsonData["id"]
            tweetDate=jsonData["created_at"]
            d=datetime.strptime(tweetDate, '%a %b %d %H:%M:%S +0000 %Y')
            yield tweetId, str(tweetId)+"\t"+str(d.strftime("%Y-%m-%d %H:%M:%S"))+"\t"+str(tweetLang)          
        except Exception, e:
            print e

    def reducer(self, key, values):
        for value in values:
            yield (key, value)

    def steps(self):
        return [self.mr(mapper=self.mapper, reducer=self.reducer),]
 

if __name__ == '__main__':
    TweetToRedshift.run()
