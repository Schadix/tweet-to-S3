# Tweet collector via tweepy and user-supplied search terms,
# to be used with CloudFormation template
# based on http://badhessian.org/2012/10/collecting-real-time-twitter-data-with-the-streaming-api/
# with modifications by http://github.com/marciw

from twaiter import TWaiter
import tweepy, sys, twitterparams
import logging
import logging.handlers
import signal

# authentication params (supplied via cfn)
consumer_key = twitterparams.OAuthConsKey
consumer_secret = twitterparams.OAuthConsSecret
access_token = twitterparams.OAuthToken
access_token_secret = twitterparams.OAuthTokenSecret

# OAuth via tweepy
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

LOG_FILENAME = "logs/output.log"
logger = logging.getLogger('RotatingLogger')
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=100000, backupCount=10)
logger.addHandler(handler)

waiter = TWaiter(api, "tweets")
stream = tweepy.Stream(auth, waiter)


def main(term):

    print "Collecting tweets. Please wait."
    logger.info("Collecting tweets. Please wait.")

    try:
        stream.sample()
    # except (KeyboardInterrupt, SystemExit):
    #     stream.disconnect()
    except Exception, e:
        print "An error occurred. No tweets collected.", e
        stream.disconnect()
        waiter.close()


def close(signal, frame):
    stream.disconnect()
    waiter.close()

if __name__ == '__main__':
    signal.signal(signal.SIGINT, close)
    signal.signal(signal.SIGTERM, close)

    main(sys.argv)
    print "before logging.shutdown"
    handlers = logger.handlers[:]
    for handler in handlers:
        print "closing handler: {0}".format(handler)
        handler.close()
        logger.removeHandler(handler)

    print "after logging shutdown"
