import sys
import re
import random
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, desc
# from nltk.corpus import stopwords
import platform

print(platform.python_version())

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext (sc)

####### functions
def findHashtags(tweet):
    regex = "#([A-Za-z_0-9]+)"
    return re.findall(regex, tweet)
def flattenByHashtag(tweetid, hashtags, country):
    result = []
    for hashtag in hashtags:
        result.append((tweetid, hashtag, country))
    return result

####### get input data into dataframe
dfTweet = sqlContext.read.option("header", True).csv("tweet_modified")
dfTweet.createOrReplaceTempView("tweet_view")
dfLocation = sqlContext.read.option("header", True).csv("location.csv")
dfLocation.createOrReplaceTempView("location_view")
queryStr = "select tweetid, lang, isreshare, retweetcount, likes, text, userid, country, city from tweet_view, location_view where tweet_view.locationid=location_view.locationid"
dfData = sqlContext.sql(queryStr)
rddData = dfData.rdd
# dfData.show()

####### get word sentiment data from file into a dict
dfSentimentRef = sqlContext.read.option("header", True).csv("SentiWordNet_3.0.0_modified_nohash.CSV")
dfSentimentRef = dfSentimentRef.select(dfSentimentRef.SynsetTerms, dfSentimentRef.PosScore, dfSentimentRef.NegScore)
dfSentimentRef = dfSentimentRef.rdd.map(lambda x: (str(x[0]).split(" "), (float(x[1]) - float(x[2])))).toDF(["words","score"])
dfSentimentRef = dfSentimentRef.select(explode(dfSentimentRef.words), dfSentimentRef.score)
rddSentimentRef = dfSentimentRef.rdd.mapValues(lambda v: (v, 1)) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .mapValues(lambda v: v[0] / v[1])
dictSentimentRef = {}
for row in rddSentimentRef.collect():
    if (row[0] in dictSentimentRef) == False:
        dictSentimentRef[row[0]] = row[1]
# for key in dictSentimentRef:
#     print(key+", "+str(dictSentimentRef[key]))
# print(dictSentimentRef['dying'])



####### calculate sentiment score for each tweet
dfTweetidText = dfData.rdd.map(lambda x: (str(x[0]), str(x[5]).split(" "))).toDF(["tweetid","words_of_text"])
dfTweetidWord = dfTweetidText.select(dfTweetidText.tweetid, explode(dfTweetidText.words_of_text))
# dfTweetidWord.show()
def findSentimentForWord(word, dictSentimentRef):
    if word in dictSentimentRef:
        return float(dictSentimentRef[word])
    else:
        return float(0)
rddTweetidWordSentiment = dfTweetidWord.rdd.map(lambda x: (x[0],findSentimentForWord(x[1], dictSentimentRef)))
# rddTweetidWordSentiment.toDF().show()
# print(rddTweetidWordSentiment.count())
rddTweetidSentiment = rddTweetidWordSentiment.reduceByKey(lambda x1, x2: x1+x2)
dfTweetidSentiment = rddTweetidSentiment.toDF(["tweetid", "sentiment"])
# dfTweetidSentiment.orderBy(("score")).show(100)
dfTweetidSentiment.write.option("header",True).csv("dfTweetidSentiment.csv")
# rddTweetidSentiment.toDF().show()

####### calculate sentiment score for each hashtag
# rddTextHashtagCountry = rddData.flatMap(lambda x: flattenByHashtag(str(x[0]), findHashtags(str(x[5])), str(x[7])))
dfTweetidHashtagsCountry = rddData.map(lambda x: (str(x[0]), findHashtags(str(x[5])), str(x[7]))).toDF(["tweetid","hashtags","country"])
dfTweetidHashtagCountry = dfTweetidHashtagsCountry.select(dfTweetidHashtagsCountry.tweetid, explode(dfTweetidHashtagsCountry.hashtags).alias('hashtag'),dfTweetidHashtagsCountry.country)
# dfTweetidHashtagCountry.show()

dfTweetidHashtagCountrySentiment = dfTweetidHashtagCountry.join(dfTweetidSentiment, dfTweetidHashtagCountry.tweetid==dfTweetidSentiment.tweetid, "inner")
# dfTweetidHashtagCountrySentiment.show(100)

rddHashtagSentiment = dfTweetidHashtagCountrySentiment.select("hashtag", "sentiment").rdd
rddHashtagSentiment = rddHashtagSentiment.reduceByKey(lambda x1, x2: x1+x2)
dfHashtagSentiment = rddHashtagSentiment.toDF(["hashtag", "sentiment"])
# dfHashtagSentiment.toDF().show(100)
dfHashtagSentiment.write.option("header",True).csv("dfHashtagSentiment.csv")

####### ???
# rddHashtagCountrySentiment = dfTweetidHashtagCountrySentiment.select("hashtag", "country", "sentiment").rdd
# rddHashtagCountrySentiment.map(lambda x: (str(x[0]+"_"+x[1]),float(x[2])))
# rddHashtagCountrySentiment = rddHashtagCountrySentiment.reduceByKey(lambda x1, x2: (x1[0]+x2[0], x1[1]+x2[1]))
# rddHashtagCountrySentiment.toDF().show(100)

# dfHS = dfData.select(dfData.text, explode(findHashtags(dfData.text)))
# dfHS.show()
# i = 0
# for row in rddTExtHashtagCountry.collect():
#     print(row)
#     i = i+1
#     if i > 100:
#         break







