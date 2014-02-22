#!/bin/bash

if [ $# -ne 1 ]
then
  echo "Usage: $0 <server>"
  exit
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
scp collector.py twaiter.py $1:tweet-to-S3/
