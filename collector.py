# Tweet collector via tweepy and user-supplied search terms,
# to be used with CloudFormation template
# based on http://badhessian.org/2012/10/collecting-real-time-twitter-data-with-the-streaming-api/
# with modifications by http://github.com/marciw

from twaiter import TWaiter
import tweepy, sys, twitterparams
import time
import logging
from daemon import runner
import os

class App():
    # authentication params (supplied via cfn)

    def __init__(self):
        logger=logging.getLogger('RotatingLogger')
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/null'
        self.stderr_path = '/dev/null'
        self.pidfile_path =  os.path.dirname(os.path.realpath(__file__))+"/tweet-to-S3.pid"
        logger.info("pidfile_path: {0}".format(self.pidfile_path))
        self.pidfile_timeout = 5

    def run(self):
        logger=logging.getLogger('RotatingLogger')
        consumer_key = twitterparams.OAuthConsKey
        consumer_secret = twitterparams.OAuthConsSecret
        access_token = twitterparams.OAuthToken
        access_token_secret = twitterparams.OAuthTokenSecret

        # OAuth via tweepy
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)

        waiter = TWaiter(api)
        stream = tweepy.Stream(auth, waiter)

        try:
            stream.sample()
        except Exception, e:
            logger.error("An error occurred. No tweets collected. {0}".format(e))
            stream.disconnect()


LOG_FILENAME = "logs/output.log"
logger = logging.getLogger('RotatingLogger')
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=100000, backupCount=10)
logger.addHandler(handler)

app = App()


daemon_runner = runner.DaemonRunner(app)
daemon_runner.daemon_context.files_preserve=[handler.stream]
daemon_runner.do_action()

