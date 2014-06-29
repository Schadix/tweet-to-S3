tweet-to-S3
===========

reads sample tweet and throws it in S3 bucket

Requirements:
* python 2.7 (for timedelta.to_seconds()) - use virtualenv
* pip install boto
* pip install tweepy
* pip install zdaemon

twitterparams.py
================
OAuthConsKey = `<Twitter Auth Key>`<br>
OAuthConsSecret = `<Twitter Secret Key>`<br>
OAuthToken = `<Twitter OAuthToken>`<br>
OAuthTokenSecret = `<Twitter OAuth Secret>`<br>
CW_NAMESPACE= `<CloudWatch Namespace>`<br>
CW_INTERVAL= `<interval to send updates to CloudWatch in s>`<br>
COUNTER_MAX_SIZE= `<max size of tweets to gather before zipping and pushing to S3`<br>
BUCKET_NAME= `<S3 bucket name>`<br>
REGION= `<region used>`<br>
ENV= `dev|prod`
CONFIG_TABLENAME='tweet-to-s3-properties'
KINESIS_STREAM='tweets'
TWEETS_COLLECTED_FOLDER='tweets/collected'
CHECK_FOLDER_FREQUENCY= in seconds
