-- pig -param input_hdfs=pig/samleTweets
data1 = load 'pig/tweets/tweets_20121102.txt' using PigStorage('|') as (x1, x2, x3, x4, x5, x6 , x7, x8, x9, tweet:chararray);
data2 = load 'pig/tweets/tweets_20121103.txt' using PigStorage('|') as (x1, x2, x3, x4, x5, x6 , x7, x8, x9, tweet:chararray);
data = UNION data1, data2;

tweet = foreach data generate tweet;

tweet_rank = RANK tweet;


token1 = FOREACH tweet_rank GENERATE rank_tweet, REPLACE(tweet,'[^A-Za-z0-9-]', ' ') AS tweet:chararray;
token2 = FOREACH token1 GENERATE rank_tweet as id, FLATTEN(TOKENIZE(LOWER(TRIM(tweet)))) AS word:chararray;

--token1 = FOREACH tweet_rank GENERATE rank_tweet, TOKENIZE(TRIM(tweet)) AS tokens: {T:(word:chararray)};
--token2 = FOREACH token1 {
    --cleaned = FOREACH tokens GENERATE 
              --FLATTEN(REGEX_EXTRACT_ALL(LOWER(word),'.*?([^A-Za-z0-9//-]+).*?')) as word ;
    --GENERATE rank_tweet as id, FLATTEN(cleaned);
--}


good = load 'pig/good.txt' as (good_word: chararray);
bad = load 'pig/bad.txt' as (bad_word: chararray);

goodScore = foreach good generate good_word, 1 as score; 
sentiment_good = join token2 by word, goodScore by good_word;
senti_good = foreach sentiment_good generate id, word, score;

badScore = foreach bad generate bad_word, -1 as score;
sentiment_bad = join token2 by word, badScore by bad_word;
senti_bad = foreach sentiment_bad generate id, word, score;

senti = UNION senti_good, senti_bad;

grouped = group senti by id;
senti_word = foreach grouped generate group as id, SUM(senti.$2) as final_score;

pos_neg = FOREACH senti_word GENERATE id, (final_score > 0 ? 'positive' : (final_score < 0 ? 'negative' : 'neutral')) AS senti;

pos_neg2 = FILTER pos_neg by senti != 'neutral';

grouped2 = group pos_neg2 by senti;

result = foreach grouped2 generate group, COUNT(pos_neg2) as freq; 

store result into 'pig/out';