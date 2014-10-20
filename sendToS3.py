import twitterparams
import time, glob, datetime, signal, os, sys, subprocess, logging
import boto
from boto.s3.key import Key
import boto.ec2.cloudwatch
from boto.dynamodb2.table import Table

def close(_a, _b):
    handlers = logger.handlers[:]
    for handler in handlers:
        logger.info("closing handler: {0}".format(handler))
        handler.close()
        logger.removeHandler(handler)
	sys.exit()

signal.signal(signal.SIGINT, close)
signal.signal(signal.SIGTERM, close)

LOG_FILENAME = "logs/sendToS3_output.log"
logger = logging.getLogger('RotatingLogger')
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=100000, backupCount=10)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(filename)s - %(levelname)s - %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

cw_interval_start=datetime.datetime.now()
current_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(current_dir)
if not os.path.exists(twitterparams.TWEETS_COLLECTED_FOLDER):
	os.makedirs(twitterparams.TWEETS_COLLECTED_FOLDER)

dynamoTable = Table(twitterparams.CONFIG_TABLENAME)
cwConn = boto.ec2.cloudwatch.connect_to_region(twitterparams.REGION)

def get_lines_in_file(filename):
	lines_in_file=sum(1 for line in open(filename))
	logger.info("get_lines_in_file: {0}".format(lines_in_file))
	return lines_in_file

def compress_file(filename):
	logger.info("compress_file: {0}".format(filename))
	proc=subprocess.Popen(["bzip2", filename], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)	
	out, err = proc.communicate()
	while (proc.returncode is None):
		out, err = proc.communicate()
		time.sleep(0.1)
	if (proc.returncode==1 and "already exists" in out):
		logger.warn("{0}".format(out))
		return "AlreadyExists"
	return "OK"

def send_to_S3(filename):
	logger.info("send_to_S3 - start {0}".format(filename))
	bzipFilename=filename+'.bz2'
	f=open(bzipFilename)
	path, name = os.path.split(os.path.abspath(f.name))
	f.close()
	k = Key(bucket)
	k.key = twitterparams.TARGETFOLDER+"/"+datetime.datetime.now().strftime("%Y-%m-%d/")+name
	k.set_contents_from_filename(bzipFilename)
	logger.info("send_to_S3 - done. source: {0}, target: {1}".format(name, k.key))

# not thread safe
def exceeds_interval():
	global cw_interval_start
	delta_seconds = (datetime.datetime.now() - cw_interval_start).seconds
	if (delta_seconds > twitterparams.CW_INTERVAL):
		cw_interval_start=datetime.datetime.now()
		return True
	else:
		return False

def update_CloudWatchAndDynamo(lines_in_file):
	logger.info("update_CloudWatchAndDynamo. lines in file: {0}".format(lines_in_file))
	item = dynamoTable.get_item(id=twitterparams.ENV+'#total_tweets')
	new_value = int(item['value'])+lines_in_file
	cwConn.put_metric_data(namespace=twitterparams.CW_NAMESPACE,name="tweetsTotal",value=new_value
	, timestamp=datetime.datetime.now(), unit="Count")
	item['value']=int(new_value)
	item.save()
	logger.info("update_CloudWatchAndDynamo. tweetsTotal: {0}".format(new_value))

#=========

s3_connect = boto.connect_s3()
bucket = s3_connect.get_bucket(twitterparams.BUCKET_NAME)

#========
sum_tweets_collected=0

while True:
	file_list=glob.glob("{0}/*.txt".format(twitterparams.TWEETS_COLLECTED_FOLDER))
	if (file_list):
		logger.info("found files: {0}".format(file_list))
		for f in file_list:
			lines_in_file=get_lines_in_file(f)
			logger.info("lines_in_file: {0}".format(lines_in_file))
			compress_return_code = compress_file(f)
			if (compress_return_code=="OK"):
				send_to_S3(f)
				sum_tweets_collected+=lines_in_file
				os.remove(f+".bz2")
			elif (compress_return_code=="AlreadyExists"):
				os.remove(f)
				os.remove(f+".bz2")
	if (exceeds_interval()):
		update_CloudWatchAndDynamo(sum_tweets_collected)
		sum_tweets_collected=0
	time.sleep(twitterparams.CHECK_FOLDER_FREQUENCY)
close()
