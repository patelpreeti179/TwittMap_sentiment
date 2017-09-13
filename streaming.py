# -*- coding: utf-8 -*-
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
import json
import boto3

access_token = "your_token"
access_token_secret = "your_token_secret"
consumer_key = "your_key"
consumer_secret = "your_secret"

keywordList = ['movies','sports','music','finance','technology','fashion','science','travel','health','cricket','india']

sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName="tweetsQueue")

def findCategory(text, keywordList):
    category = []
    for keyword in keywordList:
        if keyword in text:
            category.append(keyword)
    return category

def send_message(body):
    """ sends a message to the AWS queue """
    response = queue.send_message(MessageBody=body)
    print (("INFO: added message - %s to the queue") % response.get('MessageId'))

class StdOutListener(StreamListener):
    def __init__(self):
        self.counter = 0
        self.limit = 500
    def on_data(self, data):
        if self.counter < self.limit:
            decoded = json.loads(data)
            if decoded.get('coordinates',None) is not None:
                id = decoded['id']
                time = decoded.get('created_at','')
                text = decoded['text'].lower().encode('ascii','ignore').decode('ascii')
                coordinates = decoded.get('coordinates','').get('coordinates','')
                category = findCategory(text, keywordList)
                tweet = {'timestamp': time,
                         'text': text,
                         'coordinates': coordinates,
                         'category': category,
                         'id': id }
                self.counter += 1
                send_message(json.dumps(tweet))
        else:
            twitterStream.disconnect()

    def on_error(self, status):
        print ("error: "), status

if __name__ == '__main__':
    while True:
        l = StdOutListener()
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        twitterStream = Stream(auth, l)
        twitterStream.filter(track=['movies','sports','music','finance','technology','fashion','science','travel','health','cricket','india'])
