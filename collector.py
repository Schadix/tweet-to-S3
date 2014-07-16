# Tweet collector via tweepy and user-supplied search terms,
# to be used with CloudFormation template
# based on http://badhessian.org/2012/10/collecting-real-time-twitter-data-with-the-streaming-api/
# with modifications by http://github.com/marciw

from twaiter import TWaiter
import tweepy, sys, twitterparams, signal
import logging, logging.handlers
import boto.ec2.cloudwatch


# authentication params (supplied via cfn)
consumer_key = twitterparams.OAuthConsKey
consumer_secret = twitterparams.OAuthConsSecret
access_token = twitterparams.OAuthToken
access_token_secret = twitterparams.OAuthTokenSecret
CW_NAMESPACE = twitterparams.CW_NAMESPACE

# OAuth via tweepy
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

LOG_FILENAME = "logs/output.log"
logger = logging.getLogger('RotatingLogger')
logger.setLevel(logging.INFO)
# create formatter and add it to the handlers
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=100000, backupCount=10)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(filename)s - %(levelname)s - %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

waiter = TWaiter(api, "tweets")
stream = tweepy.Stream(auth, waiter)

def main(term):

    logger.info("Starting to collect tweets")

    try:
        stream.sample()
    # except (KeyboardInterrupt, SystemExit):
    #     stream.disconnect()
    except Exception, e:
        logger.error("An error occurred. No tweets collected. {0}".format(e))
        stream.disconnect()
        waiter.close()
        try:
            cwConn = boto.ec2.cloudwatch.connect_to_region(region)
            cwConn.put_metric_data(namespace=self.CW_NAMESPACE,name="tweetException",value=1, timestamp=datetime.datetime.now(), unit="Count")
        except Exception, ex:
            logger.error("couldn't send cloudwatch update for exception. {0}".format(ex))
        sys.exit(1)


def close(signal, frame):
    stream.disconnect()
    waiter.close()

if __name__ == '__main__':
    signal.signal(signal.SIGINT, close)
    signal.signal(signal.SIGTERM, close)

    main(sys.argv)
    handlers = logger.handlers[:]
    for handler in handlers:
        logger.info("closing handler: {0}".format(handler))
        handler.close()
        logger.removeHandler(handler)

