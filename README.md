# CS5344 Project
## Code
### How to run
```bash
spark-submit test1.py
```

### Input
* **tweet_modified.csv**: pre-processed tweet data file
* **location.csv**: location data file
* **SentiWordNet_3.0.0_modified_nohash.CSV**: word sentiment score reference

### Output
* **dfHashtagSentiment.csv** folder containing output csvs of sentiment scores of each hashtags
* **dfTweetidSentiment.csv** folder containing output csvs of sentiment scores of each tweets
### Some insights to learn

### Logic
1. load tweet data and location data and join into dataframe dfData
2. load word sentiment data from word sentiment score reference and load into dict dictSentimentRef
3. calculate sentiment score for each tweet, store in dfTweetidSentiment
4. calculate sentiment score for each hashtag, store in dfHashtagSentiment



