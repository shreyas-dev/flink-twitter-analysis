# flink-twitter-analysis
Flink Twitter Analysis Using Kafka

## Description

Capture Live Twitter Tweets and search for tweets with specific pattern or regex

## Usage

``mvn clean package``

---
``flink run /path/to/jar --consumerKey urConsumerKey --consumerSecret urConsumerSecret --token urToken --tokenSecret urTokenSecret --pattern "interested"``
___

You can get your consumer configs and token details from twitter developer portal -> https://developer.twitter.com/en/apps/