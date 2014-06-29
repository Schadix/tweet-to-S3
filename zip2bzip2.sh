#!/bin/bash
# ./zip2bzip2.sh lists/xaq > xaq.log 2>&1 &
# helper to migrate from zip to bzip2 for hadoop processing

inputfile=$1
echo "processing: $inputfile"

while read line; do
  tpath=`echo $line | awk -F "/" '{ print $1"/"$2 }'`
  tname=`echo $line | awk -F "/" '{ print $3 }'`
  echo "line: $line, tname: $tname"
  # mkdir -p $tpath
  aws s3 cp s3://sdx-tweets/$line $tname
  unzippedName=`unzip -Z -1 $tname`
  unzip $tname
  bzip2 -v $unzippedName
  echo "bzip2 finished $unzippedName"
  aws s3 cp $unzippedName.bz2 s3://sdx-tweets/$tpath/
  echo "finished: $line"
done < $inputfile