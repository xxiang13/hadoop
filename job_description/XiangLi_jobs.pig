--data1 = load 'pig/jobs/20140212_descriptions.csv' using PigStorage(',') as (id:chararray, ads:chararray);
--data2 = load 'pig/jobs/20140213_descriptions.csv' using PigStorage(',') as (id:chararray, ads:chararray);
--data = UNION data1, data2;

data = load 'pig/jobs/sampleJobs.csv' using PigStorage(',') as (id:chararray, ads:chararray);

token1 = FOREACH data GENERATE id, REPLACE(ads,'[^A-Za-z0-9-\']', ' ') AS ads:chararray;
token2 = FOREACH token1 GENERATE id, FLATTEN(TOKENIZE(LOWER(TRIM(ads)))) AS ads:chararray;

REGISTER checkUDF.jar;

checkNumber = FOREACH token2 GENERATE id as id, ads as ads, checkUDF(ads) as bool:chararray;
checkNumber2 = FILTER checkNumber by (bool == 'false');
checkNumber3 = FILTER checkNumber by (bool == 'true');

getWord = FOREACH checkNumber2 GENERATE id as id, ads as ads;
getNumber = FOREACH checkNumber3 GENERATE id as id, ads as ads;

stopWord = load 'pig/jobs/stopwords-en.txt' as (word:chararray);

removeStop1 = JOIN getWord by ads Left Outer, stopWord by word using 'replicated';

removeStop2 = FILTER removeStop1 by word is NULL;

removeStop3 = FOREACH removeStop2 GENERATE id as id, ads as ads;

REGISTER stemUDF.jar;

stemmer = FOREACH removeStop3 GENERATE id as id, stemUDF(ads) as ads:chararray;

dict = load 'pig/jobs/dictionary.txt' as (dict:chararray);

checkMisspell1 = JOIN stemmer by ads Left Outer, dict by dict using 'replicated';

correctWord1 = FILTER checkMisspell1 by dict is not NULL;
correctWord2 = FOREACH correctWord1 GENERATE id as id, dict as dict;

checkMisspell2 = FILTER checkMisspell1 by dict is NULL;
checkMisspell3 = FOREACH checkMisspell2 GENERATE id, ads;
checkMisspell4 = CROSS checkMisspell3, dict;

REGISTER LevenUDF.jar;
checkMisspell5 = FOREACH checkMisspell4 GENERATE id as id, ads as ads, dict as dict, LevenUDF(ads, dict) as distance: int;

grouped = group checkMisspell5 by (id, ads);

getRrightWord = FOREACH grouped{
				x = ORDER checkMisspell5 BY distance asc;
				y = limit x 1;
				generate y;
				};

getRrightWord2 = FOREACH getRrightWord GENERATE FLATTEN(y.$0) as id, FLATTEN(y.$2) as dict;

allWord = union getRrightWord2, correctWord2, getNumber;

grouped1 = group allWord by id;

result = FOREACH grouped1 GENERATE group as id, allWord.$1 as word;


store result into 'pig/jobs/out';


--grouped = group correctWord1 ALL;
--count1 = FOREACH grouped generate COUNT(correctWord1);
--grouped1 = group checkMisspell2 ALL;
--count2 = FOREACH grouped1 generate COUNT(checkMisspell2);




--http://www.cob.unt.edu/itds/faculty/evangelopoulos/dsci5910/Porter.java
--http://snowball.tartarus.org/algorithms/porter/stemmer.html
--http://www.eecis.udel.edu/~trnka/CISC889-11S/lectures/dan-porters.pdf
--http://alvinalexander.com/java/jwarehouse/lucene-1.3-final/src/java/org/apache/lucene/analysis/PorterStemmer.java.shtml
--https://github.com/vialab/sentiment-state/blob/master/backend/String/src/StringToken/Stemmer.java