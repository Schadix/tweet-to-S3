# based on http://badhessian.org/2012/10/collecting-real-time-twitter-data-with-the-streaming-api/
# with modifications by http://github.com/marciw
# requires Tweepy https://github.com/tweepy/tweepy

from tweepy import StreamListener
from os import rename, remove
import os
import subprocess
import json, time, sys
import uuid
import datetime
import time
import boto
from boto.s3.key import Key
import boto.ec2.cloudwatch
import logging
import twitterparams


class TWaiter(StreamListener):

    logger=logging.getLogger('RotatingLogger')

    BUCKET_NAME = twitterparams.BUCKET_NAME
    # see Tweepy for more info

    def get_filename(self):
        folder = 'tweets/{0}'.format(time.strftime("%Y%m%d"))
        if not os.path.exists(folder):
            os.makedirs(folder)
        filename = ('{0}/tweet.{1}.txt').format(folder, 
            datetime.datetime.now().strftime("%Y%m%d-%H:%M:%S.%f"))
        return filename

    def __init__(self, api = None):
        self.api = api or API()
        self.counter = 0
        self.number_of_tweeets = 0
        self.interval = twitterparams.CW_INTERVAL
        self.counter_max_size = twitterparams.COUNTER_MAX_SIZE
        self.CW_NAMESPACE = twitterparams.CW_NAMESPACE
        try:
            current_dir = os.path.dirname(os.path.realpath(__file__))
            os.chdir(current_dir)
            self.output  = open(self.get_filename(), 'w')
        except Exception, e:
            self.logger.error("Problem opening output file. Excpetion: {0}".format(e))
        s3Conn = boto.connect_s3()
        self.bucket = s3Conn.get_bucket(self.BUCKET_NAME)
        self.cwConn = boto.ec2.cloudwatch.connect_to_region('us-east-1')
        self.tweet_read_starttime = datetime.datetime.now()
        self.tweet_interval_start_count = self.number_of_tweeets

    def on_data(self, data):
        if  'in_reply_to_status' in data:
            self.on_status(data)


    def on_status(self, status):
        try:
            self.output.write(status)
            self.counter += 1
            self.number_of_tweeets += 1

            total_seconds = (datetime.datetime.now() - self.tweet_read_starttime).total_seconds()
            if (total_seconds > self.interval):
                try:
                    count_per_second = (self.number_of_tweeets - self.tweet_interval_start_count) / total_seconds
                    self.logger.info("self.number_of_tweeets: {0}, self.tweet_interval_start_count: {1}, total_seconds: {2}, count_per_second: {3}".format(self.number_of_tweeets, self.tweet_interval_start_count, total_seconds, count_per_second))
                    self.cwConn.put_metric_data(namespace=self.CW_NAMESPACE,name="tweetsPerSecond",value=count_per_second
                        , timestamp=datetime.datetime.now(), unit="Count/Second")
                    self.cwConn.put_metric_data(namespace=self.CW_NAMESPACE,name="tweetsTotal",value=self.number_of_tweeets
                        , timestamp=datetime.datetime.now(), unit="Count")
                    self.tweet_read_starttime=datetime.datetime.now()
                    self.tweet_interval_start_count = self.number_of_tweeets
                except Exception, e:
                    self.logger.error("wrong. Exception {0}".format(e))

            if self.counter >= self.counter_max_size:
                # TODO: should go into subprocess
                # 1. create new file (leave old file open for writes)
                # 2. switch to new file
                self.counter = 0
                old_temp_file = self.output
                self.output.close()
                self.output  = open(self.get_filename(), 'w')
                subprocess.call(["zip", old_temp_file.name+'.zip', old_temp_file.name])
                remove(old_temp_file.name)
                k = Key(self.bucket)
                k.key = old_temp_file.name+'.zip'
                k.set_contents_from_filename(old_temp_file.name+'.zip')
                self.logger.info('{0}.zip copied to S3 bucket'.format(old_temp_file.name))
                remove(old_temp_file.name+'.zip')
        except Exception, e:
            self.logger.error("wrong. Exception {0}".format(e))
        return

    def on_delete(self, status_id, user_id):
        #self.deleted.write(str(status_id) + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Error: ' + str(status_code) + "\n")
        return False
