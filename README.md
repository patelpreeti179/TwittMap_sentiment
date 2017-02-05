# TwittMap_sentiment
Objectives
Use the Amazon SQS service to create a processing queue for the Tweets that are delivered by the Twitter Streaming API
Use Amazon SNS service to update the status processing on each tweet so the UI can refresh.
Integrate a third party cloud service API into the Tweet processing flow.
Overview

Streaming
Reads a stream of tweets from the Twitter Streaming API. Note: you might follow a set of specific keywords that you find interesting
After fetching a new tweet, check to see if it has geolocation info and is in English.
Once the tweet validates these filters, send a message to SQS for asynchronous processing on the text of the tweet
Worker

Define a worker pool that will pick up messages from the queue to process. These workers should each run on a separate pool thread.
Make a call to the sentiment API off your preference (e.g. Alchemy). This can return a positive, negative or neutral sentiment evaluation for the text of the submitted Tweet.
As soon as the tweet is processed send a notification -using SNS- to an HTTP endpoint that contains the information about the tweet.
Backend

On receiving the notification, index this tweet in Elasticsearch. Make sure you preserve the sentiment of the tweet as well.
The backend should provide the functionality to the user to search for tweets that match a particular keyword. To this end, you can either write a new webserver or simply reuse your first assignment.
Frontend

When a new tweet is indexed, provide some visual indication on the frontend.
Give the user the ability to search your index via a free text input or a dropdown.
Plot the tweets that match the query on a map. (use markers)
Lastly, use a custom marker to indicate the sentiment. Alternatively, you can come up with any other style to represent the sentiment of the tweet. 
Deployment

Deploy your application, the streaming script, the worker etc. on AWS.
You are free to use Elastic beanstalk or manually configure EC2 for deploying different components of your application.

Architecture Diagram:

<img width="1219" alt="screen shot 2017-02-05 at 4 21 45 pm" src="https://cloud.githubusercontent.com/assets/16263680/22629985/3eecf280-ebbf-11e6-851e-0fbc8c599273.png">
