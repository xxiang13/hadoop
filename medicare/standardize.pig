--Load the data set, PigStorage('\t') means separating by tab

Data = load 'clustering/Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt' using PigStorage('\t') as (npi, nppes_provider_last_org_name, nppes_provider_first_name, nppes_provider_mi, nppes_credentials, nppes_provider_gender, nppes_entity_code, nppes_provider_street1, nppes_provider_street2, nppes_provider_city, nppes_provider_zip, nppes_provider_state, nppes_provider_country, provider_type, medicare_participation_indicator, place_of_Service, hcpcs_code, hcpcs_description, line_srvc_cnt:double, bene_unique_cnt:double, bene_day_srvc_cnt:double, average_Medicare_allowed_amt:double, stdev_Medicare_allowed_amt:double, average_submitted_chrg_amt:double, stdev_submitted_chrg_amt:double, average_Medicare_payment_amt:double, stdev_Medicare_payment_amt:double);

--Since there is NA in column 18. Filter out all rows with NA
dataNona = filter Data by $18 is not NULL;

#get column 18-26 from the file
medicare = foreach dataNona generate $18, $19, $20, $21, $22, $23, $24, $25, $26;

store medicare into 'medicare';

--compute stadarization by using x-xmin/xmax-xmin ###

--group columns by one key
grouped = group medicare all;

--get max value for each column
max = foreach grouped generate MAX(medicare.$18),MAX(medicare.$19),MAX(medicare.$20),MAX(medicare.$21),MAX(medicare.$22),MAX(medicare.$23),MAX(medicare.$24),MAX(medicare.$25),MAX(medicare.$26);

--get min value for each column

min = foreach grouped generate MIN(medicare.$18),MIN(medicare.$19),MIN(medicare.$20),MIN(medicare.$21),MIN(medicare.$22),MIN(medicare.$23),MIN(medicare.$24),MIN(medicare.$25),MIN(medicare.$26);

--compute standarization
medicare2 = foreach medicare generate ($18 - min.$0)/(max.$0-min.$0), ($19 - min.$1)/(max.$1-min.$1), ($20 - min.$2)/(max.$2-min.$2), ($21 - min.$3)/(max.$3-min.$3),($22 - min.$4)/(max.$4-min.$4),($23 - min.$5)/(max.$5-min.$5),($24 - min.$6)/(max.$6-min.$6),($25 - min.$7)/(max.$7-min.$7),($26 - min.$8)/(max.$8-min.$8);

--save data
store medicare2 into 'clustering/medicare_standarized';



