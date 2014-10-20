# Tweet collector via tweepy and user-supplied search terms,
# to be used with CloudFormation template
# based on http://badhessian.org/2012/10/collecting-real-time-twitter-data-with-the-streaming-api/
# with modifications by http://github.com/marciw

from twaiter import TWaiter
import tweepy, sys, twitterparams, signal
import logging, logging.handlers
import sys, getopt

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

# TODO
# logfile, cw metric, zdaemon socket name


def main(term):
    DEFAULT="sample"
    filtervalue=DEFAULT
    namevalue=DEFAULT
    try:
        opts, args = getopt.getopt(term[1:],"hf:n:",["filter=", "name="])
        print opts, args
    except getopt.GetoptError:
        print 'test.py -f <track=[phase1,phase2,phase3,...]|locations=[x.y,x,y]|follow=[name1,name2,name3,...]> -n <name>'
        sys.exit(2)
    for opt, arg in opts:
        print opt, arg
        if opt in ('-f', '--filter'):
            filtervalue=arg
        elif opt in ('-n', '--name'):
            namevalue=arg
        else:
            print "no args, using 'sample' stream as default"
    if (filtervalue==DEFAULT) ^ (namevalue==DEFAULT):
        print "When using a filter, also give it a unique name"
        sys.exit(1)


    LOG_FILENAME = "logs/{0}-output.log".format(namevalue)
    logger = logging.getLogger('RotatingLogger')
    logger.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=100000, backupCount=10)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(filename)s - %(levelname)s - %(lineno)d - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info("Starting to collect tweets")
    logger.info("filtervalue: {0}, namevalue: {1}".format(filtervalue, namevalue))

    waiter = TWaiter(api, namevalue)
    stream = tweepy.Stream(auth, waiter)
    try:
        if filtervalue==DEFAULT:
            logger.info("sample stream")
            stream.sample()
        elif (filtervalue.startswith("locations=")):
            logger.info("filter: {0}".format(filtervalue))
            loc=[float(x) for x in filtervalue[11:-1].split(",")]
            logger.info("loc: {0}".format(loc))
            stream.filter(locations=loc)
        elif (filtervalue.startswith("follow=")):
            logger.info("filter: {0}".format(filtervalue))
            stream.filter(follow=filtervalue[8:-1].split(","))
        elif (filtervalue.startswith("track=")):
            logger.info("filter: {0}".format(filtervalue))
            stream.filter(track=filtervalue[7:-1].split(","))
        else:
            log.error("no match for filtervalue: {0}:".format(filtervalue))
        stream.disconnect()
    except Exception, e:
        logger.error("An error occurred. No tweets collected. {0}".format(e))
        waiter.close()
        sys.exit(1)


def close(signal, frame):
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

