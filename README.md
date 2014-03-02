tweet-to-S3
===========

reads sample tweet and throws it in S3 bucket

Requirements:
* python 2.7 (for timedelta.to_seconds()) - use virtualenv
* pip install boto
* pip install tweepy
* pip install python-daemon
* pip install zdaemon

twitterparams.py
================
OAuthConsKey = <Twitter Auth Key>
OAuthConsSecret = <Twitter Secret Key>
OAuthToken = <Twitter OAuthToken>
OAuthTokenSecret = <Twitter OAuth Secret>
CW_NAMESPACE='tweet-to-s3'
CW_INTERVAL=<interval to send updates to CloudWatch>
COUNTER_MAX_SIZE=<max size of tweets to gather before zipping and pushing to S3
BUCKET_NAME=<S3 bucket name>
REGION=<region used>

