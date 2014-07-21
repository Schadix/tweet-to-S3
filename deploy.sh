#!/bin/bash

APP_FOLDER="tweet-to-S3"

if [ $# -ne 1 ]
then
  echo "Usage: $0 <server>"
  exit
fi

ssh $1 ' 
mkdir -p tweet-to-S3
'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
scp collector.py twaiter.py sendToS3.py init.d/tweet-zipper init.d/tweet-to-S3 init.d/tweet-geo conf/zdaemon.conf conf/zdaemon-zipper.conf conf/zdaemon-geo.conf $1:$APP_FOLDER/

ssh -t $1 "
sudo cp $APP_FOLDER/tweet-zipper /etc/init.d/
sudo cp $APP_FOLDER/tweet-to-S3 /etc/init.d/
sudo cp $APP_FOLDER/tweet-geo /etc/init.d/
"
