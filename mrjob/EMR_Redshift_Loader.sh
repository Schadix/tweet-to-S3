#!/bin/bash
 
# TODO(READER): set these variables first
export INPUT_S3="s3://sdx-tweets/bzip2/"
export LOG_S3="s3://sdx-logs/emr-redshift-loader/"
export OUTPUT_S3="s3://tweet-emr-output/emr-redshift-loader/"
 
nohup python EMR_Redshift_Loader.py --ssh-tunnel-to-job-tracker --jobconf mapreduce.output.compress=true --ssh-tunnel-is-closed --ec2-instance-type=m1.small --no-output --enable-emr-debugging --ami-version=latest --s3-log-uri=${LOGS_S3} -o ${OUTPUT_S3} -r emr ${INPUT_S3} --num-ec2-instances=1 &

