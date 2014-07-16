#!/bin/bash

if [ $# -ne 1 ]
then
  echo "Usage: $0 <server>"
  exit
fi

ssh $1 <<'ENDSSH'
mkdir -p tweet-to-S3
ENDSSH

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
scp collector.py twaiter.py sendToS3.py init.d/tweet-zipper init.d/tweet-to-S3 conf/zdaemon.conf conf/zdaemon-zipper.conf $1:tweet-to-S3/

ssh $1 <<'ENDSSH'
sudo cp tweet-zipper /etc/init.d/
sudo cp tweet-to-S3 /etc/init.d/
ENDSSH
