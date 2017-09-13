import boto3
import gevent
import json
import random
from elasticsearch import Elasticsearch, exceptions
import requests

WORKERS = 10
API_URL = "http://gateway-a.watsonplatform.net/calls/text/TextGetTextSentiment"
API_TOKEN = "your_token"
QUEUE_NAME = "your_queue_name"
WAIT_TIME = 10 # time to wait between each SQS poll
TOPIC_NAME = "your_topic_name"
SNS_ARN = "your_snr_arn"

sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
sns = boto3.client('sns')

es = Elasticsearch()

def task(pid):
    print ("[Task %s] Starting ..." % pid)
    while True:
        for message in queue.receive_messages():
            tweet = json.loads(message.body)
            payload = {
                'apikey': API_TOKEN, "outputMode": "json", "text": tweet["text"]
            }
            r = requests.get(API_URL, params=payload)
            if r.status_code == 200 and r.json().get("status") != "ERROR":
                tweet["sentiment"] = r.json().get("docSentiment")
                # index tweet in ES
                res = es.index(index="idx-ankit", doc_type="idx-ank", id=tweet["id"], body=tweet)

                # send notification
                sns.publish(
                    TopicArn=SNS_ARN,
                    Message=json.dumps(tweet),
                    Subject='New Tweet')

                print ("[Task %s] Tweet %s indexed" % (pid, tweet["id"]))
            message.delete()
        gevent.sleep(WAIT_TIME)


if __name__ == "__main__":
    threads = [gevent.spawn(task, pid) for pid in range(1, WORKERS+1)]
    gevent.joinall(threads)
