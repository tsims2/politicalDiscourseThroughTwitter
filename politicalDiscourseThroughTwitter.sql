-- Databricks notebook source
-- DBTITLE 1,Creating Database
--For this analysis I am creating a database for all python and SQL querries. I downloaded some of the outputs from this analysis to sue with Tableua for vizuals.
DROP DATABASE IF EXISTS CongressionalTweets CASCADE;
CREATE DATABASE CongressionalTweets;
USE CongressionalTweets;

-- COMMAND ----------

-- DBTITLE 1,Initial Data Pull
--This cell is pulling the raw datafiles from the filestore to be used in this analysis.
--This data is from twitter.com
CREATE OR REPLACE TEMPORARY VIEW usersBronze
USING parquet
OPTIONS (
  path "/FileStore/tables/users.parquet/"
);

--Pulling data on the members of congress received from congress.gov. This data covers all memebers of congress, Representatives and Senators, from the years 2007 - 2017. 
CREATE OR REPLACE TEMPORARY VIEW membersBronze
USING parquet
OPTIONS (
  path "/user/hive/warehouse/congressrecords"
);

--Pulling data on tweets associated with Representatives, Senators, Governors, and the Office of the President. Due to the size of the json file the data was nested in, there were issues uploading the data so I had to break the file apart using a terminal prompt. 
CREATE OR REPLACE TEMPORARY VIEW tweetsaeaa
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeaa"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeab
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeab"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeac
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeac"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaead
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaead"
);


CREATE OR REPLACE TEMPORARY VIEW tweetsaeae
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeae"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeaf
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeaf"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeag
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeag"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeah
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeah"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeai
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeai"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeaj
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeaj"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeak
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeak"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeal
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeal"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeam
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeam"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaean
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaean"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeao
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeao"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeap
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeap"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeaq
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeaq"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaear
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaear"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeas
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeas"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeat
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeat"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeau
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeau"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeav
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeav"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeaw
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeaw"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeax
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeax"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeay
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeay"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeaz
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeaz"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaeba
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaeba"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaebb
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaebb"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaebc
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaebc"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaebd
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaebd"
);

CREATE OR REPLACE TEMPORARY VIEW tweetsaebe
USING parquet
OPTIONS (
  path "/user/hive/warehouse/tweetsaebe"
);
CREATE OR REPLACE TEMPORARY VIEW sentiment1
USING parquet
OPTIONS (
  path "/user/hive/warehouse/sentiment1_csv"
);
CREATE OR REPLACE TEMPORARY VIEW sentiment2
USING parquet
OPTIONS (
  path "/user/hive/warehouse/sentiment2_csv"
);

-- COMMAND ----------

-- DBTITLE 1,Combining Tweet Files
--This cell is creating uncleaned tables for all the tweets, hashtags, and mentions.
DROP TABLE IF EXISTS tweetsBronze;
CREATE TABLE tweetsBronze USING DELTA
AS  SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeaa UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeab UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeac UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaead UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeae UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeaf UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeag UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeah UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeai UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeaj UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeak UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeal UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeam UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaean UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeao UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeap UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeaq UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaear UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeas UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeat UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeau UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeav UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeaw UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeax UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeay UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeaz UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaeba UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaebb UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaebc UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaebd UNION ALL
    SELECT user_id, screen_name, id AS tweet_id, DATE(FROM_UNIXTIME(created_at)) as created_at, retweet_count, favorite_count, text, truncated FROM tweetsaebe;
    
DROP TABLE IF EXISTS hashtagsBronze;
CREATE TABLE hashtagsBronze USING DELTA
AS(                        SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeaa);
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeab;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeac;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaead;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeae;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeaf;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeag;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeah;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeai;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeaj;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeak;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeal;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeam;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaean;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeao;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeap;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeaq;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaear;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeas;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeat;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeau;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeav;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeaw;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeax;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeay;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeaz;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaeba;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaebb;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaebc;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaebd;
INSERT INTO hashtagsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.hashtags) AS hashtag FROM tweetsaebe;

DROP TABLE IF EXISTS mentionsBronze;
CREATE TABLE mentionsBronze USING DELTA
AS(                        SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeaa);
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeab;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeac;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaead;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeae;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeaf;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeag;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeah;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeai;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeaj;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeak;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeal;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeam;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaean;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeao;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeap;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeaq;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaear;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeas;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeat;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeau;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeav;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeaw;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeax;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeay;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeaz;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaeba;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaebb;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaebc;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaebd;
INSERT INTO mentionsBronze SELECT user_id, id AS tweet_id, EXPLODE(entities.user_mentions) AS mention FROM tweetsaebe;

-- COMMAND ----------

-- DBTITLE 1,Congressional Twitter Accounts
--This cell is creating a new DataFrame on Twitter accounts data that will be more useful in creating business-level tables. 
--My goal was to get the legal name for all the accounts to link them to the congressional data.
DROP TABLE IF EXISTS users;
CREATE TABLE users
USING DELTA
AS 
  SELECT 
    name,
    id,
    screen_name,
    DATE(FROM_UNIXTIME(created_at)) as created_at,
    statuses_count,
    listed_count,
    favourites_count as favorites_count,
    followers_count,
    element_at(entities.url.urls.display_url,1) as url,
    description
  FROM
    usersBronze
  WHERE
    verified = TRUE;

--This querry is replacing non name objects in the name field.
UPDATE users
SET name = CASE WHEN name LIKE '%Governor%' THEN CONCAT(ELEMENT_AT(SPLIT(name, " "),2), " ", ELEMENT_AT(SPLIT(name, " "),3))
                WHEN name LIKE 'Gov%' THEN CONCAT(ELEMENT_AT(SPLIT(name, " "),2), " ", ELEMENT_AT(SPLIT(name, " "),3))
                WHEN name LIKE '%Rep%' THEN CONCAT(ELEMENT_AT(SPLIT(name, " "),2), " ", ELEMENT_AT(SPLIT(name, " "),3))
                WHEN name LIKE '%Sen%' THEN CONCAT(ELEMENT_AT(SPLIT(name, " "),2), " ", ELEMENT_AT(SPLIT(name, " "),3))
                WHEN name LIKE 'Governor%' THEN CONCAT(ELEMENT_AT(SPLIT(name, " "),2), " ", ELEMENT_AT(SPLIT(name, " "),3))
                WHEN name LIKE 'Congress%' THEN CONCAT(ELEMENT_AT(SPLIT(name, " "),2), " ", ELEMENT_AT(SPLIT(name, " "),3))
                WHEN name LIKE 'Dr%' THEN CONCAT(ELEMENT_AT(SPLIT(name, " "),2), " ", ELEMENT_AT(SPLIT(name, " "),3))
                WHEN name LIKE 'Con.%' THEN CONCAT(ELEMENT_AT(SPLIT(name, " "),2), " ", ELEMENT_AT(SPLIT(name, " "),3))
                WHEN name LIKE '% % %' THEN CONCAT(ELEMENT_AT(SPLIT(name, " "),1), " ", ELEMENT_AT(SPLIT(name, " "),3))
                WHEN name LIKE '% %' THEN CONCAT(ELEMENT_AT(SPLIT(name, " "),1), " ", ELEMENT_AT(SPLIT(name, " "),2))
                END
WHERE name LIKE '%';

--After comparing this DataFrame to the congress data set, I made these changes.
UPDATE users SET name = 'Bennie Thompson' WHERE id = 82453460;
UPDATE users SET name = 'Patrick Tiberi' WHERE id = 3145255727;
UPDATE users SET name = 'Seth Moulton' WHERE id = 3091316093;
UPDATE users SET name = 'Dan Sullivan' WHERE id = 2891210047;
UPDATE users SET name = 'Carol Shea-Porter' WHERE id = 126171100;
UPDATE users SET name = 'Scott Perry' WHERE id = 18773159;
UPDATE users SET name = 'Michelle Grisham' WHERE id = 1061385474;
UPDATE users SET name = 'Kevin Brady' WHERE id = 19926675;
UPDATE users SET name = 'Jerrold Nadler' WHERE id = 40302336;
UPDATE users SET name = 'Jodey Arrington' WHERE id = 816284664658874368;
UPDATE users SET name = 'Mark Amodei' WHERE id = 402719755;
UPDATE users SET name = 'John Cornyn' WHERE id = 13218102;
UPDATE users SET name = 'James Sensenbrenner' WHERE id = 851621377;
UPDATE users SET name = 'Grace Napolitano' WHERE id = 161411080;
UPDATE users SET name = 'Charles Grassley' WHERE id = 10615232;
UPDATE users SET name = 'Rodgers McMorris' WHERE id = 17976923;
UPDATE users SET name = 'Vicente Gonzalez' WHERE id = 818536152588238849;
UPDATE users SET name = 'Richard Nolan' WHERE id = 1055685948;
UPDATE users SET name = 'Billy Long' WHERE id = 1444015610;
UPDATE users SET name = 'Kathy Castor' WHERE id = 1880674038;
UPDATE users SET name = 'Rodney Davis' WHERE id = 993153006;
UPDATE users SET name = 'Michael Turner' WHERE id = 51228911;
UPDATE users SET name = 'Eddie Johnson' WHERE id = 168502762;
UPDATE users SET name = 'Donald Payne' WHERE id = 1155335864;
UPDATE users SET name = 'Drew Ferguson' WHERE id = 806583915012046854;
UPDATE users SET name = 'Andre Carson' WHERE id = 199325935;
UPDATE users SET name = 'Andy Harris' WHERE id = 960962340;
UPDATE users SET name = 'Anna Eshoo' WHERE id = 249348006;
UPDATE users SET name = 'Bernard Sanders' WHERE id = 29442313;
UPDATE users SET name = 'Bradley Schneider' WHERE id = 1071840474;
UPDATE users SET name = 'Clay Higgins' WHERE id = 3298489662;
UPDATE users SET name = 'Carolyn Maloney' WHERE id = 258900199;
UPDATE users SET name = 'Charles Dent' WHERE id = 242376736;
UPDATE users SET name = 'Michael Simpson' WHERE id = 76132891;
UPDATE users SET name = 'Wasserman Schultz' WHERE id = 1140648348;
UPDATE users SET name = 'Richard Durbin' WHERE id = 247334603;
UPDATE users SET name = 'George Butterfield' WHERE id = 432676344;
UPDATE users SET name = 'Gerald Connolly' WHERE id = 78445977;
UPDATE users SET name = 'Gregg Harper' WHERE id = 20334348;
UPDATE users SET name = 'Harold Rogers' WHERE id = 550401754;
UPDATE users SET name = 'Orrin Hatch' WHERE id = 262756641;
UPDATE users SET name = 'John Reed' WHERE id = 486694111;
UPDATE users SET name = 'Jacklyn Rosen' WHERE id = 4749863113;
UPDATE users SET name = 'Janice Schakowsky' WHERE id = 24195214;
UPDATE users SET name = 'Claudia Tenney' WHERE id = 797201048490430465;
UPDATE users SET name = 'Karen Bass' WHERE id = 239949176;
UPDATE users SET name = 'Jay Inslee' WHERE id = 21789463;
UPDATE users SET name = 'Terri Sewell' WHERE id = 381152398;
UPDATE users SET name = 'Steve Chabot' WHERE id = 237750442;
UPDATE users SET name = 'Stevan Pearce' WHERE id = 235271965;
UPDATE users SET name = 'Andre Carson' WHERE id = 199325935;
UPDATE users SET name = 'Sander Levin' WHERE id = 25781141;
UPDATE users SET name = 'Henry Johnson' WHERE id = 24745957;
UPDATE users SET name = 'John Carter' WHERE id = 18030431;
UPDATE users SET name = 'Larry Bucshon' WHERE id = 234812598;
UPDATE users SET name = 'Mitch McConnell' WHERE id = 1249982359;
UPDATE users SET name = 'Luis Correa' WHERE id = 815985039485837312;
UPDATE users SET name = 'Mac Thornberry' WHERE id = 377534571;
UPDATE users SET name = 'Paul Gosar' WHERE id = 240760644;
UPDATE users SET name = 'Sanford Bishop' WHERE id = 249410485;
UPDATE users SET name = 'Ruben Kihuen' WHERE id = 816282029276938240;
UPDATE users SET name = 'Brendan Boyle' WHERE id = 231108733;
UPDATE users SET name = 'Al Lawson' WHERE id = 818472418620608512;
UPDATE users SET name = 'US President' WHERE id = 822215679726100480;
UPDATE users SET name = 'Paul Gosar' WHERE id = 240760644;
UPDATE users SET name = 'John Conyers' WHERE id = 138770045;
UPDATE users SET name = 'Christopher Christie' WHERE id = 90484508;
UPDATE users SET name = 'Brian Sandoval' WHERE id = 208931294;
UPDATE users SET name = 'Eric Crawford' WHERE id = 233693291;
UPDATE users SET name = 'Robert Latta' WHERE id = 15394954;
UPDATE users SET name = 'David Roe' WHERE id = 52503751;
UPDATE users SET name = 'Lisa Rochester' WHERE id = 817050219007328258;
UPDATE users SET name = 'John Edwards' WHERE id = 166374616;
UPDATE users SET name = 'Matthew Bevin' WHERE id = 3141213592;
UPDATE users SET name = 'Matthew Mead' WHERE id = 2303719598;
UPDATE users SET name = 'Nanette Barragan' WHERE id = 816833925456789505;
UPDATE users SET name = 'Raul Labrador' WHERE id = 246341769;
UPDATE users SET name = 'Raul Grijalva' WHERE id = 28602948;

--I am creating a temporary table to help clean the name colum. I am changing out common nick names with the name that would appear in the congress DataFrame.
DROP TABLE IF EXISTS tempuser;
CREATE TABLE tempuser
USING DELTA
AS
  SELECT 
    id,
    ELEMENT_AT(SPLIT(u.name, " "),1) AS firstname,
    u.name,
    m.name AS mname,
    screen_name
  FROM
    users AS u
  LEFT OUTER JOIN
    membersBronze AS m
  ON 
    u.name=m.name
  WHERE
    m.name IS NULL;

UPDATE tempuser
SET firstname = CASE WHEN firstname LIKE 'Mike' THEN 'Michael'
                     WHEN firstname LIKE 'Bill' THEN 'William'
                     WHEN firstname LIKE 'Chris' THEN 'Christopher'
                     WHEN firstname LIKE 'Dave' THEN 'David'
                     WHEN firstname LIKE 'Phil' THEN 'Phillip'
                     WHEN firstname LIKE 'Rick' THEN 'Richard'
                     WHEN firstname LIKE 'Alex' THEN 'Alexander'
                     WHEN firstname LIKE 'Bob' THEN 'Robert'
                     WHEN firstname LIKE 'Chuck' THEN 'Charles'
                     WHEN firstname LIKE 'Dan' THEN 'Daniel'
                     WHEN firstname LIKE 'Dick' THEN 'Dan'
                     WHEN firstname LIKE 'Don' THEN 'Donald'
                     WHEN firstname LIKE 'Ed' THEN 'Edward'
                     WHEN firstname LIKE 'Jim' THEN 'James'
                     WHEN firstname LIKE 'Joe' THEN 'Joseph'
                     WHEN firstname LIKE 'Lou' THEN 'Louis'
                     WHEN firstname LIKE 'Pat' THEN 'Patrick'
                     WHEN firstname LIKE 'Pete' THEN 'Peter'
                     WHEN firstname LIKE 'Rob' THEN 'Robert'
                     WHEN firstname LIKE 'Ted' THEN 'Theodore'
                     WHEN firstname LIKE 'Thom' THEN 'Thomas'
                     WHEN firstname LIKE 'Tom' THEN 'Thomas'
                     WHEN firstname LIKE 'Tim' THEN 'Timothy'
                     ELSE firstname
                     END
WHERE firstname LIKE '%';

--Combining the temperory table with the table.
MERGE INTO 
    users AS u
USING 
    (SELECT 
      id,
      CONCAT(firstname," ",ELEMENT_AT(SPLIT(name, " "),2)) AS name
    FROM
      tempuser) AS t
ON 
    u.id = t.id 
WHEN MATCHED THEN
   UPDATE 
      SET u.name = t.name;

-- COMMAND ----------

-- DBTITLE 1,Congressional Members
--This cell is creating a new DataFrame on Congressional data that will be more useful in creating business-level tables. 
DROP TABLE IF EXISTS membersClean;
CREATE TABLE membersClean
USING DELTA
AS 
  SELECT 
    *
  FROM membersBronze;
  
--I am adding missing politicians whose twitter accounts are in the analysis.
INSERT INTO TABLE membersClean VALUES('Donald Trump', 'President', 'Republican', 'US');
INSERT INTO TABLE membersClean VALUES('US President', 'President', 'None', 'US');
INSERT INTO TABLE membersClean VALUES('Brian Sandoval', 'Governor', 'Republican', 'Nevada');
INSERT INTO TABLE membersClean VALUES('William Walker', 'Governor', 'Independent', 'Alaska');
INSERT INTO TABLE membersClean VALUES('Christopher Christie', 'Governor', 'Republican', 'New Jersey');
INSERT INTO TABLE membersClean VALUES('William Haslam', 'Governor', 'Republican', 'Tennessee');
INSERT INTO TABLE membersClean VALUES('Terry McAuliffe', 'Governor', 'Democratic', 'Virginia');
INSERT INTO TABLE membersClean VALUES('Susana Martinez', 'Governor', 'Republican', 'New Mexico');
INSERT INTO TABLE membersClean VALUES('Steve Bullock', 'Governor', 'Democratic', 'Montana');
INSERT INTO TABLE membersClean VALUES('Scott Walker', 'Governor', 'Republican', 'Wisconsin');
INSERT INTO TABLE membersClean VALUES('Richard Scott', 'Governor', 'Republican', 'Florida');
INSERT INTO TABLE membersClean VALUES('Phillip Bryant', 'Governor', 'Republican', 'Mississippi');
INSERT INTO TABLE membersClean VALUES('Paul LePage', 'Governor', 'Republican', 'Maine');
INSERT INTO TABLE membersClean VALUES('Mark Dayton', 'Governor', 'Democratic', 'Minnesota');
INSERT INTO TABLE membersClean VALUES('John Kasich', 'Governor', 'Republican', 'Ohio');
INSERT INTO TABLE membersClean VALUES('Jerry Brown', 'Governor', 'Democratic', 'California');
INSERT INTO TABLE membersClean VALUES('Gina Raimondo', 'Governor', 'Democratic', 'Rhode Island');
INSERT INTO TABLE membersClean VALUES('Gary Herbert', 'Governor', 'Republican', 'Utah');
INSERT INTO TABLE membersClean VALUES('Eric Greitens', 'Governor', 'Republican', 'Missouri');
INSERT INTO TABLE membersClean VALUES('Dennis Daugaard', 'Governor', 'Republican', 'South Dakota');
INSERT INTO TABLE membersClean VALUES('Daniel Malloy', 'Governor', 'Democratic', 'Connecticut');
INSERT INTO TABLE membersClean VALUES('Charlie Baker', 'Governor', 'Republican', 'Massachusetts');
INSERT INTO TABLE membersClean VALUES('Bruce Rauner', 'Governor', 'Republican', 'Illinois');
INSERT INTO TABLE membersClean VALUES('Andrew Cuomo', 'Governor', 'Democratic', 'New York');
INSERT INTO TABLE membersClean VALUES('Lisa Rochester', 'Representative', 'Democratic', 'NewJersey');
INSERT INTO TABLE membersClean VALUES('Matthew Bevin', 'Governor', 'Republican', 'Kentucky');
INSERT INTO TABLE membersClean VALUES('Matthew Mead', 'Governor', 'Republican', 'Wyoming');
INSERT INTO TABLE membersClean VALUES('Richard Snyder', 'Governor', 'Republican', 'Michigan');
INSERT INTO TABLE membersClean VALUES('Raul Grijalva', 'Representative', 'Democratic', 'Arizona');

--Further name cleaning.
UPDATE membersClean SET name = 'Christopher Hollen' WHERE name = 'Hollen, Van';
UPDATE membersClean SET name = 'Cortez Masto' WHERE name = 'Masto, Cortez';
UPDATE membersClean SET name = 'Wasserman Schultz' WHERE name = 'Schultz, Wasserman';
UPDATE membersClean SET name = 'Frank Pallone' WHERE name = 'Frank, Pallone';
UPDATE membersClean SET name = 'John Conyers' WHERE name = 'John, Conyers';
UPDATE membersClean SET name = NULL where name = 'Jay Inslee' AND Political_Office = 'Representative';
UPDATE membersClean SET name = 'Rodgers McMorris' WHERE name = 'Rodgers, McMorris';

--Deleting politicians who appear twice in the dataset because they held different offices over the course of the study. I chose to keep the last office held by the politician.
DELETE FROM membersClean WHERE name = 'Jay Inslee%' AND Political_Office = 'Representative';
DELETE FROM membersClean WHERE name = 'Donald Payne';
DELETE FROM membersClean WHERE name = 'Ron DeSantis' AND Political_Office = 'Governor';
DELETE FROM membersClean WHERE name = 'Jared Polis' AND Political_Office = 'Governor';
INSERT INTO TABLE membersClean VALUES('Donald Payne', 'Representative', 'Democratic', 'New Jersey');
DELETE FROM membersClean WHERE name = 'John Carney' AND Political_Office = 'Governor';
DELETE FROM membersClean WHERE name = 'Duncan Hunter';
INSERT INTO TABLE membersClean VALUES('Duncan Hunter', 'Representative', 'Republican', 'California');
DELETE FROM membersClean WHERE name = 'Mike Rogers' AND State = 'Michigan';

--This querry is making a table combining congresisonal data with the user id from tweeter.
DROP TABLE IF EXISTS members;
CREATE TABLE members
USING DELTA
AS(
  SELECT
    u.id,
    u.name,
    m.Political_Office,
    m.Party,
    m.state
  FROM 
    users AS u
  LEFT JOIN
    membersClean AS m
  ON 
    u.name=m.name
)

-- COMMAND ----------

-- DBTITLE 1,Tweets
--This cell is creating a new DataFrame on Tweets data that will be more useful in creating business-level tables. 
DROP TABLE IF EXISTS tweets;
CREATE TABLE tweets
USING DELTA
AS(
  SELECT 
    t.user_id,
    t.screen_name,
    t.tweet_id,
    t.created_at,
    t.retweet_count,
    t.favorite_count,
    t.text
  FROM 
    tweetsBronze AS t
  INNER JOIN
    users AS u
  ON 
    t.user_id = u.id
  WHERE
    truncated = 'False'
  )

-- COMMAND ----------

-- DBTITLE 1,Hashtags
--This cell is creating a new DataFrame for hashtags from the tweets dataset.
DROP TABLE IF EXISTS hashtags;
CREATE TABLE hashtags
USING DELTA
AS(
  SELECT 
    h.user_id,
    h.tweet_id,
    h.hashtag.text AS hashtag,
    t.created_at
  FROM
    hashtagsBronze AS h
  INNER JOIN
    users AS u
  ON
    u.id = h.user_id
  INNER JOIN
    tweets AS t
  ON
    t.tweet_id = h.tweet_id
);

-- COMMAND ----------

-- DBTITLE 1,Mentions
--This cell is creating a new DataFrame for mentions from the tweets dataset.
DROP TABLE IF EXISTS mentions;
CREATE TABLE mentions
USING DELTA
AS(
  SELECT 
    m.user_id,
    m.tweet_id,
    m.mention.id AS mention_user_id,
    m.mention.name AS mention_name,
    m.mention.screen_name AS mention_screen_name,
    t.created_at
  FROM
    mentionsBronze AS m
  INNER JOIN
    users AS u
  ON
    u.id = m.user_id
  INNER JOIN
    tweets AS t
  ON
    t.tweet_id = m.tweet_id
);

-- COMMAND ----------

-- DBTITLE 1,Tweet Sentiment
'''This cell is using python to clean the text field in tweets and determin sentiment.'''
%python
#--------------------------------------------------------------------------------------------
%pip install -U textblob
%pip install pyspark.pandas
%pip install emoji --upgrade
#--------------------------------------------------------------------------------------------
from pyspark.sql.functions import col, split, lit
from textblob import TextBlob
from pyspark.sql.types import *
from html.parser import HTMLParser
from nltk.sentiment import SentimentIntensityAnalyzer
import pyspark.sql.functions as F
import pandas
import re
import emoji
import nltk
import string
nltk.download([
  "names",
  "words",
  "stopwords",
  "averaged_perceptron_tagger",
  "vader_lexicon",
  "punkt",
])
stop_words = set(nltk.corpus.stopwords.words('english'))
words = set(nltk.corpus.words.words())
#--------------------------------------------------------------------------------------------
def cleaned(t):
  t = t.lower()
  t = " ".join([word for word in t.split() if word not in stop_words])
  tEncode = t.encode(encoding="ascii", errors="ignore")
  tDecode = tEncode.decode("utf-8")
  t = " ".join([word for word in tDecode.split()])
  t = re.sub("@\S+", "", t)
  t = re.sub("\$", "", t)
  t = re.sub("https?:\/\/.*[\r\n]*", "", t)
  t = re.sub("#", "", t)
  punct = set(string.punctuation) 
  t = "".join([ch for ch in t if ch not in punct])
  return t
spark.udf.register("cleanedWithPython", cleaned)
cleaned = F.udf(cleaned, StringType())
#--------------------------------------------------------------------------------------------
'''I used two popular sentiment analysis. I then tested them against a human eye using random tweets. I decided on using the NLTk analysis.'''
def sentimentBlob(t):
  return TextBlob(t).sentiment.polarity
spark.udf.register("sentimentBlobWithPython", sentimentBlob)
sentimentBlob = F.udf(sentimentBlob, StringType())
#--------------------------------------------------------------------------------------------
def sentimentNLTK(t):
  sent = SentimentIntensityAnalyzer()
  return sent.polarity_scores(t)['compound']
spark.udf.register("sentimentNLTKWithPython", sentimentNLTK)
sentimentNLTK = F.udf(sentimentNLTK, StringType())
#--------------------------------------------------------------------------------------------
df = spark.sql("SELECT  user_id, tweet_id, text, created_at FROM tweets")
df2 = df.withColumn("cleanText", cleaned("text"))
df3 = df2.withColumn("sentimentBlob", sentimentBlob("cleanText"))
#--------------------------------------------------------------------------------------------
df4 = df3.withColumn("sentimentNLTK", sentimentNLTK("cleanText"))
#--------------------------------------------------------------------------------------------
#df6.write.saveAsTable('sentimentTemp')
df4.createOrReplaceTempView("sentimentTemp")

-- COMMAND ----------

--This cell is creating a table to use in business-level analysis.
DROP TABLE IF EXISTS sentiment;
CREATE TABLE sentiment
USING DELTA
AS
  (SELECT
    user_id,
    tweet_id,
    CASE    WHEN sentimentNLTK > 0.0  THEN 'Positive'
            WHEN sentimentNLTK < 0.0 THEN 'Negative'
            ELSE 'Neutral'
            END
            AS sentiment,
    created_at
  FROM 
    sentimentTemp)

-- COMMAND ----------

-- DBTITLE 1,Word Frequency
--This cell is calculating word freq.
SELECT 
  word,
  COUNT(*) AS count
FROM
  (SELECT
    EXPLODE(SPLIT(cleanText, ' ')) AS word
  FROM
    sentimentTemp)
WHERE
  word NOT LIKE 'rt'
AND
  word NOT LIKE 'amp'
AND
  word NOT LIKE 'today'
AND
  word NOT LIKE 'us'
AND
  word NOT LIKE 'w'
GROUP BY
  word
ORDER BY
  count DESC

-- COMMAND ----------

-- DBTITLE 1,Data In Observation
--This cell is calculating congress members in the analysis.
SELECT
  COUNT(*) AS Congresspeople
FROM 
  users

-- COMMAND ----------

--This cell is calculating tweets in the analysis.
SELECT
  COUNT(*) AS tweets
FROM
  tweets

-- COMMAND ----------

--This cell is calculating hashtags in the analysis.
SELECT 
  COUNT(*) AS Hashtags
FROM
  hashtags

-- COMMAND ----------

--This cell is calculating mentions in the analysis.
SELECT 
  COUNT(*) AS Mentions
FROM
  mentions

-- COMMAND ----------

-- DBTITLE 1,Tweets by Year
--This cell is calculating tweets by year.
SELECT
  DATE_FORMAT(created_at, "y") AS year,
  COUNT(*) AS tweets
FROM
  tweets
GROUP BY
  year

-- COMMAND ----------

-- DBTITLE 1,Tweets by Month
--This cell is calculating tweets by month.
SELECT
  DATE_FORMAT(created_at, 'MMM') AS month,
  COUNT(*) AS tweets
FROM
  tweets
WHERE
  DATE_FORMAT(created_at, 'y') < 2017
GROUP BY
  month

-- COMMAND ----------

-- DBTITLE 1,Tweets by Day
--This cell is calculating tweets by day of the week.
SELECT
  DATE_FORMAT(created_at, 'EEE') AS day,
  COUNT(*) AS tweets
FROM
  tweets
WHERE
  DATE_FORMAT(created_at, 'y') < 2017
GROUP BY
  day

-- COMMAND ----------

-- DBTITLE 1,Congressional Breakdown
--This cell is looking at party representaiton.
SELECT 
  Party,
  COUNT(*) AS Members_Represented
FROM
  members
WHERE 
  name NOT LIKE 'US President'
GROUP BY
  Party
ORDER BY
  Members_Represented DESC

--Based on this output representation from independents and libertatians is to small to group them in future party anlysis. 

-- COMMAND ----------

-- DBTITLE 1,Tweet Statistics By Politician
--This cell is creating a table with the count of tweets for each congress member.
DROP TABLE IF EXISTS tweetCount;
CREATE TABLE tweetCount
USING DELTA
AS(
 SELECT 
    user_id,
    screen_name,
    political_office,
    party,
    COUNT(DISTINCT tweet_id) AS total_tweets
  FROM 
    tweets AS t
  LEFT OUTER JOIN 
    members AS m
  ON 
    t.user_id = m.id
  WHERE 
    id IS NOT NULL
  GROUP BY
    user_id,
    screen_name,
    political_office,
    party
);

SELECT
*
FROM
tweetCount

-- COMMAND ----------

-- DBTITLE 1,Tweet Statistics
SELECT
  SUM(total_tweets)      AS total_tweets,
  INT(AVG(total_tweets)) AS mean,
  MIN(total_tweets)      AS min,
  MAX(total_tweets)      AS max,
  INT(STDDEV_POP(total_tweets)) AS std
FROM 
  tweetCount

-- COMMAND ----------

-- DBTITLE 1,Tweet Statistics By Political Party
SELECT 
  party,
  SUM(total_tweets)             AS total_tweets,
  INT(AVG(total_tweets))        AS mean,
  MIN(total_tweets)             AS min,
  MAX(total_tweets)             AS max,
  INT(STDDEV_POP(total_tweets)) AS std
FROM 
  tweetCount
WHERE
  party = 'Republican'
OR
  party = 'Democratic'
GROUP BY
  party

-- COMMAND ----------

-- DBTITLE 1,Tweet Statistics By Political Office
SELECT 
  political_office,
  SUM(total_tweets)      AS total_tweets,
  INT(AVG(total_tweets)) AS mean,
  MIN(total_tweets)      AS min,
  MAX(total_tweets)      AS max,
  INT(STDDEV_POP(total_tweets)) AS std
FROM 
  tweetCount
GROUP BY
  political_office

-- COMMAND ----------

-- DBTITLE 1,Hashtag Count
--What are the most talked about things
SELECT 
  LOWER(hashtag) AS hashtag,
  COUNT(*) AS count
FROM 
  hashtags
GROUP BY
  LOWER(hashtag)
ORDER BY
  count DESC

-- COMMAND ----------

-- DBTITLE 1,Top 10 Republican Trending Hashtags
SELECT
  hashtag,
  count,
  CAST(FORMAT_NUMBER((negative/count)*100,2) AS FLOAT) AS negative,
  CAST(FORMAT_NUMBER((neutral/count)*100,2) AS FLOAT) AS neutral,
  CAST(FORMAT_NUMBER((positive/count)*100,2) AS FLOAT) AS positive
FROM
---------------------------------------------------------------------------------------------
  (SELECT
    hashtag,
    SUM(count) AS count,
    SUM(CASE WHEN sentiment = 'Negative' THEN count
    END) AS negative,
    SUM(CASE WHEN sentiment = 'Neutral' THEN count
    END) AS neutral,
    SUM(CASE WHEN sentiment = 'Positive' THEN count
    END) AS positive
  FROM
---------------------------------------------------------------------------------------------
    (SELECT 
      LOWER(hashtag) AS hashtag,
      party,
      sentimentNLTK AS sentiment,
      count(*) AS count
    FROM 
      hashtags AS h
    LEFT JOIN
      members AS m
    ON 
      m.id = h.user_id
    LEFT JOIN
      sentiment AS s
    ON 
      h.tweet_id = s.tweet_id
    WHERE
      party = 'Republican'
    GROUP BY 
      hashtag,
      party,
      sentimentNLTK
    ORDER BY
      count DESC
      ) AS h
---------------------------------------------------------------------------------------------
  GROUP BY
    hashtag)
---------------------------------------------------------------------------------------------
ORDER BY 
  count DESC
LIMIT
  25

-- COMMAND ----------

-- DBTITLE 1,Top 10 Democratic Trending Hashtags
SELECT
  hashtag,
  count,
  CAST(FORMAT_NUMBER((negative/count)*100,2) AS FLOAT) AS negative,
  CAST(FORMAT_NUMBER((neutral/count)*100,2) AS FLOAT) AS neutral,
  CAST(FORMAT_NUMBER((positive/count)*100,2) AS FLOAT) AS positive
FROM
---------------------------------------------------------------------------------------------
  (SELECT
    hashtag,
    SUM(count) AS count,
    SUM(CASE WHEN sentiment = 'Negative' THEN count
    END) AS negative,
    SUM(CASE WHEN sentiment = 'Neutral' THEN count
    END) AS neutral,
    SUM(CASE WHEN sentiment = 'Positive' THEN count
    END) AS positive
  FROM
---------------------------------------------------------------------------------------------
    (SELECT 
      LOWER(hashtag) AS hashtag,
      party,
      sentimentNLTK AS sentiment,
      count(*) AS count
    FROM 
      hashtags AS h
    LEFT JOIN
      members AS m
    ON 
      m.id = h.user_id
    LEFT JOIN
      sentiment AS s
    ON 
      h.tweet_id = s.tweet_id
    WHERE
      party = 'Democratic'
    GROUP BY 
      hashtag,
      party,
      sentiment
    ORDER BY
      count DESC
      ) AS h
---------------------------------------------------------------------------------------------
  GROUP BY
    hashtag)
---------------------------------------------------------------------------------------------
ORDER BY 
  count DESC
LIMIT
  25

-- COMMAND ----------

-- DBTITLE 1,Top 10 Congress Contacts for Republicans
SELECT
  screen_name,
  mention_party,
  count,
  CAST(FORMAT_NUMBER((negative/count)*100,2) AS FLOAT) AS negative,
  CAST(FORMAT_NUMBER((neutral/count)*100,2) AS FLOAT) AS neutral,
  CAST(FORMAT_NUMBER((positive/count)*100,2) AS FLOAT) AS positive
FROM
---------------------------------------------------------------------------------------------
  (SELECT
    mention_screen_name AS screen_name,
    SUM(count) AS count,
    mention_party,
    MAX(CASE WHEN sentiment = 'Negative' THEN count
    END) AS negative,
    MAX(CASE WHEN sentiment = 'Neutral' THEN count
    END) AS neutral,
    MAX(CASE WHEN sentiment = 'Positive' THEN count
    END) AS positive
  FROM
---------------------------------------------------------------------------------------------
    (SELECT
      mention_id,
      mention_screen_name,
      mention_party,
      sentiment,
      party,
      COUNT(*) AS count
    FROM
---------------------------------------------------------------------------------------------
      (SELECT 
        id AS mention_id,
        mention_screen_name,
        COUNT(*) AS mention_count
      FROM 
        mentions AS m
      INNER JOIN
        users AS u
      ON
        mention_screen_name = screen_name
      GROUP BY 
        id,
        mention_screen_name
      ORDER BY
        mention_count DESC
      ) AS p
---------------------------------------------------------------------------------------------
    LEFT JOIN
---------------------------------------------------------------------------------------------
     (SELECT 
        m.user_id,
        m.mention_user_id,
        s.sentimentNLTK AS sentiment,
        mem.party AS party,
        memb.party AS mention_party
      FROM 
        mentions AS m
      LEFT JOIN
        sentiment as s
      ON
        m.tweet_id = s.tweet_id
      LEFT JOIN
        members AS mem
      ON
        m.user_id = mem.id
      LEFT JOIN
        members AS memb
      ON
        m.mention_user_id = memb.id) AS t
---------------------------------------------------------------------------------------------
    ON
      p.mention_id = mention_user_id
    WHERE
      party = 'Republican'
    GROUP BY
      mention_id,
      mention_screen_name,
      sentiment, 
      mention_party,
      party)
---------------------------------------------------------------------------------------------
  GROUP BY
    mention_id,
    mention_screen_name,
    mention_party)
---------------------------------------------------------------------------------------------
ORDER BY 
  count DESC
LIMIT
  10

-- COMMAND ----------

-- DBTITLE 1,Top 10 Congressional Contacts for Democrats
SELECT
  screen_name,
  mention_party,
  count,
  CAST(FORMAT_NUMBER((negative/count)*100,2) AS FLOAT) AS negative,
  CAST(FORMAT_NUMBER((neutral/count)*100,2) AS FLOAT) AS neutral,
  CAST(FORMAT_NUMBER((positive/count)*100,2) AS FLOAT) AS positive
FROM
---------------------------------------------------------------------------------------------
  (SELECT
    mention_screen_name AS screen_name,
    SUM(count) AS count,
    mention_party,
    MAX(CASE WHEN sentiment = 'Negative' THEN count
    END) AS negative,
    MAX(CASE WHEN sentiment = 'Neutral' THEN count
    END) AS neutral,
    MAX(CASE WHEN sentiment = 'Positive' THEN count
    END) AS positive
  FROM
---------------------------------------------------------------------------------------------
    (SELECT
      mention_id,
      mention_screen_name,
      mention_party,
      sentiment,
      party,
      COUNT(*) AS count
    FROM
---------------------------------------------------------------------------------------------
      (SELECT 
        id AS mention_id,
        mention_screen_name,
        COUNT(*) AS mention_count
      FROM 
        mentions AS m
      INNER JOIN
        users AS u
      ON
        mention_screen_name = screen_name
      GROUP BY 
        id,
        mention_screen_name
      ORDER BY
        mention_count DESC
      ) AS p
---------------------------------------------------------------------------------------------
    LEFT JOIN
---------------------------------------------------------------------------------------------
     (SELECT 
        m.user_id,
        m.mention_user_id,
        s.sentimentNLTK AS sentiment,
        mem.party AS party,
        memb.party AS mention_party
      FROM 
        mentions AS m
      LEFT JOIN
        sentiment AS s
      ON
        m.tweet_id = s.tweet_id
      LEFT JOIN
        members AS mem
      ON
        m.user_id = mem.id
      LEFT JOIN
        members AS memb
      ON
        m.mention_user_id = memb.id) AS t
---------------------------------------------------------------------------------------------
    ON
      p.mention_id = mention_user_id
    WHERE
      party = 'Democratic'
    GROUP BY
      mention_id,
      mention_screen_name,
      sentiment, 
      mention_party,
      party)
---------------------------------------------------------------------------------------------
  GROUP BY
    mention_id,
    mention_screen_name,
    mention_party)
---------------------------------------------------------------------------------------------
ORDER BY 
  count DESC
LIMIT
  10

-- COMMAND ----------

-- DBTITLE 1,Top 10 Non-congressional Contacts for Republicans
SELECT
  screen_name,
  count,
  CAST(FORMAT_NUMBER((negative/count)*100,2) AS FLOAT) AS negative,
  CAST(FORMAT_NUMBER((neutral/count)*100,2) AS FLOAT) AS neutral,
  CAST(FORMAT_NUMBER((positive/count)*100,2) AS FLOAT) AS positive
FROM
---------------------------------------------------------------------------------------------
  (SELECT
    mention_screen_name AS screen_name,
    SUM(count) AS count,
    MAX(CASE WHEN sentiment = 'Negative' THEN count
    END) AS negative,
    MAX(CASE WHEN sentiment = 'Neutral' THEN count
    END) AS neutral,
    MAX(CASE WHEN sentiment = 'Positive' THEN count
    END) AS positive
  FROM
---------------------------------------------------------------------------------------------
    (SELECT
      p.mention_user_id,
      mention_screen_name,
      sentiment,
      party,
      COUNT(*) AS count
    FROM
---------------------------------------------------------------------------------------------
      (SELECT 
        m.mention_user_id,
        m.mention_screen_name,
        COUNT(*) AS count
      FROM 
        mentions AS m
      LEFT OUTER JOIN
        users AS u
      ON
        mention_screen_name = screen_name
      WHERE
        screen_name IS NULL
      AND 
        mention_screen_name <> 'SpeakerRyan'
      AND 
        mention_screen_name <> 'SpeakerBoehner'
      AND 
        mention_screen_name <> 'BarackObama'
      GROUP BY
        mention_user_id,
        mention_screen_name,
        screen_name) AS p
---------------------------------------------------------------------------------------------
    LEFT JOIN
---------------------------------------------------------------------------------------------
     (SELECT 
        m.user_id,
        m.mention_user_id,
        s.sentimentNLTK AS sentiment,
        mem.party AS party
      FROM 
        mentions AS m
      LEFT JOIN
        sentiment AS s
      ON
        m.tweet_id = s.tweet_id
      LEFT JOIN
        members AS mem
      ON
        m.user_id = mem.id) AS t
---------------------------------------------------------------------------------------------
    ON
      p.mention_user_id = t.mention_user_id
    WHERE
      party = 'Republican'
    GROUP BY
      p.mention_user_id,
      mention_screen_name,
      sentiment, 
      party)
---------------------------------------------------------------------------------------------
  GROUP BY
    mention_user_id,
    mention_screen_name)
---------------------------------------------------------------------------------------------
ORDER BY 
  count DESC
LIMIT
  10

-- COMMAND ----------

-- DBTITLE 1,Top 10 Non-congressional Contacts for Democrats
SELECT
  screen_name,
  count,
  CAST(FORMAT_NUMBER((negative/count)*100,2) AS FLOAT) AS negative,
  CAST(FORMAT_NUMBER((neutral/count)*100,2) AS FLOAT) AS neutral,
  CAST(FORMAT_NUMBER((positive/count)*100,2) AS FLOAT) AS positive
FROM
---------------------------------------------------------------------------------------------
  (SELECT
    mention_screen_name AS screen_name,
    SUM(count) AS count,
    MAX(CASE WHEN sentiment = 'Negative' THEN count
    END) AS negative,
    MAX(CASE WHEN sentiment = 'Neutral' THEN count
    END) AS neutral,
    MAX(CASE WHEN sentiment = 'Positive' THEN count
    END) AS positive
  FROM
---------------------------------------------------------------------------------------------
    (SELECT
      p.mention_user_id,
      mention_screen_name,
      sentiment,
      party,
      COUNT(*) AS count
    FROM
---------------------------------------------------------------------------------------------
      (SELECT 
        m.mention_user_id,
        m.mention_screen_name,
        COUNT(*) AS count
      FROM 
        mentions AS m
      LEFT OUTER JOIN
        users AS u
      ON
        mention_screen_name = screen_name
      WHERE
        screen_name IS NULL
      AND 
        mention_screen_name <> 'SpeakerRyan'
      AND 
        mention_screen_name <> 'SpeakerBoehner'
      AND 
        mention_screen_name <> 'BarackObama'
      GROUP BY
        mention_user_id,
        mention_screen_name,
        screen_name) AS p
---------------------------------------------------------------------------------------------
    LEFT JOIN
---------------------------------------------------------------------------------------------
     (SELECT 
        m.user_id,
        m.mention_user_id,
        s.sentimentNLTK AS sentiment,
        mem.party AS party
      FROM 
        mentions AS m
      LEFT JOIN
        sentiment AS s
      ON
        m.tweet_id = s.tweet_id
      LEFT JOIN
        members AS mem
      ON
        m.user_id = mem.id) AS t
---------------------------------------------------------------------------------------------
    ON
      p.mention_user_id = t.mention_user_id
    WHERE
      party = 'Democratic'
    GROUP BY
      p.mention_user_id,
      mention_screen_name,
      sentiment, 
      party)
---------------------------------------------------------------------------------------------
  GROUP BY
    mention_user_id,
    mention_screen_name)
---------------------------------------------------------------------------------------------
ORDER BY 
  count DESC
LIMIT
  10

-- COMMAND ----------

-- DBTITLE 1,Sentiment Statistics
SELECT
  SentimentNLTK AS sentiment,
  COUNT(*) AS Count
FROM 
  Sentiment
GROUP BY
  Sentiment

-- COMMAND ----------

-- DBTITLE 1,Sentiment Test
--This output was downloaded and opened in excel to test the results against a human eye.
SELECT
  text,
  SentimentBlob,
  SentimentNLTK
FROM 
  Sentiment AS s
LEFT JOIN
  tweets AS t
ON
  s.tweet_id = t.tweet_id
ORDER BY
  RAND()
LIMIT 
  50

-- COMMAND ----------

-- DBTITLE 1,Sentiment Statistics by Political Party
SELECT
  Party,
  SentimentNLTK,
  COUNT(*) AS Count
FROM 
  sentiment AS s
LEFT JOIN
  members AS m
ON
  s.user_id = m.id
WHERE
  party = 'Republican'
OR
  party = 'Democratic'
GROUP BY
  Party,
  Sentiment

-- COMMAND ----------

-- DBTITLE 1,Sentiment Statistics by Political Party By Month
SELECT
  Party,
  Sentiment,
  DATE_FORMAT(created_at, 'MMM yyyy') AS month,
  COUNT(*) AS Count
FROM 
  sentiment AS s
LEFT JOIN
  members AS m
ON
  s.user_id = m.id
WHERE
  party = 'Republican'
OR
  party = 'Democratic'
GROUP BY
  Party,
  Sentiment,
  month

-- COMMAND ----------

-- DBTITLE 1,Sentiment Statistics by Political Office
SELECT
  Political_Office,
  Sentiment,
  COUNT(*) AS Count
FROM 
  sentiment AS s
LEFT JOIN
  members AS m
ON
  s.user_id = m.id
GROUP BY
  Political_Office,
  Sentiment

-- COMMAND ----------

-- DBTITLE 1,Sentiment Statistics Republican to Democrat
SELECT
  sentiment,
  party,
  mention_party,
  CAST(FORMAT_NUMBER((mention_total/(SUM(mention_total) OVER()))*100,2) AS FLOAT) AS percent
FROM
  (SELECT
    s.sentimentNLTK AS sentiment,
    m.party,
    m.mention_party,
    COUNT(*) AS mention_total
  FROM
--3) This querry pulls in member information for the people mentioned.
    (SELECT
      m.user_id,
      m.tweet_id,
      m.party,
      m.mention_user_id,
      m.mention_screen_name,
      mem.party AS mention_party
    FROM
--2) The second inner most querry pulls in member information, specifically party affiliation.
      (SELECT
        m.user_id,
        m.tweet_id,
        mem.party,
        m.mention_user_id,
        m.mention_screen_name
      FROM
--1) The inner most querry is filtering out mentions not in congress or mentioned by themselves.
        (SELECT 
          m.user_id,
          m.tweet_id,
          m.mention_user_id,
          m.mention_screen_name
        FROM 
          mentions as m
        LEFT JOIN
          users as u
        ON 
          m.user_id = u.id
        WHERE 
          m.user_id <> m.mention_user_id
        AND m.mention_user_id IN (SELECT id FROM users)) AS m
--1) ------------------------------------------------------------------------------------------------
      LEFT JOIN
        members AS mem
      ON 
        m.user_id = mem.id) AS m
--2) ------------------------------------------------------------------------------------------------
    LEFT JOIN
      members AS mem
    ON 
      m.mention_user_id = mem.id) AS m
--3) ---------------------------------------------------------------------------------------------------
  LEFT JOIN
    sentiment AS s
  ON
    m.tweet_id = s.tweet_id
  WHERE
    party = 'Republican'
  AND
    mention_party = 'Democratic'
  GROUP BY
    s.sentimentNLTK,
    m.party,
    m.mention_party
    )
GROUP BY
  sentiment,
  party,
  mention_party,
  mention_total

-- COMMAND ----------

-- DBTITLE 1,Sentiment Statistics Republican to Republican
SELECT
  sentiment,
  party,
  mention_party,
  CAST(FORMAT_NUMBER((mention_total/(SUM(mention_total) OVER()))*100,2) AS FLOAT) AS percent
FROM
  (SELECT
    s.sentiment,
    m.party,
    m.mention_party,
    COUNT(*) AS mention_total
  FROM
--3) This querry pulls in member information for the people mentioned.
    (SELECT
      m.user_id,
      m.tweet_id,
      m.party,
      m.mention_user_id,
      m.mention_screen_name,
      mem.party AS mention_party
    FROM
--2) The second inner most querry pulls in member information, specifically party affiliation.
      (SELECT
        m.user_id,
        m.tweet_id,
        mem.party,
        m.mention_user_id,
        m.mention_screen_name
      FROM
--1) The inner most querry is filtering out mentions not in congress or mentioned by themselves.
        (SELECT 
          m.user_id,
          m.tweet_id,
          m.mention_user_id,
          m.mention_screen_name
        FROM 
          mentions as m
        LEFT JOIN
          users as u
        ON 
          m.user_id = u.id
        WHERE 
          m.user_id <> m.mention_user_id
        AND m.mention_user_id IN (SELECT id FROM users)) AS m
--1) ------------------------------------------------------------------------------------------------
      LEFT JOIN
        members AS mem
      ON 
        m.user_id = mem.id) AS m
--2) ------------------------------------------------------------------------------------------------
    LEFT JOIN
      members AS mem
    ON 
      m.mention_user_id = mem.id) AS m
--3) ---------------------------------------------------------------------------------------------------
  LEFT JOIN
    sentiment AS s
  ON
    m.tweet_id = s.tweet_id
  WHERE
    party = 'Republican'
  AND
    mention_party = 'Republican'
  GROUP BY
    s.sentiment,
    m.party,
    m.mention_party
    )
GROUP BY
  sentiment,
  party,
  mention_party,
  mention_total

-- COMMAND ----------

-- DBTITLE 1,Sentiment Statistics Democrat to Democrat
SELECT
  sentiment,
  party,
  mention_party,
  CAST(FORMAT_NUMBER((mention_total/(SUM(mention_total) OVER()))*100,2) AS FLOAT) AS percent
FROM
  (SELECT
    s.sentiment,
    m.party,
    m.mention_party,
    COUNT(*) AS mention_total
  FROM
--3) This querry pulls in member information for the people mentioned.
    (SELECT
      m.user_id,
      m.tweet_id,
      m.party,
      m.mention_user_id,
      m.mention_screen_name,
      mem.party AS mention_party
    FROM
--2) The second inner most querry pulls in member information, specifically party affiliation.
      (SELECT
        m.user_id,
        m.tweet_id,
        mem.party,
        m.mention_user_id,
        m.mention_screen_name
      FROM
--1) The inner most querry is filtering out mentions not in congress or mentioned by themselves.
        (SELECT 
          m.user_id,
          m.tweet_id,
          m.mention_user_id,
          m.mention_screen_name
        FROM 
          mentions as m
        LEFT JOIN
          users as u
        ON 
          m.user_id = u.id
        WHERE 
          m.user_id <> m.mention_user_id
        AND m.mention_user_id IN (SELECT id FROM users)) AS m
--1) ------------------------------------------------------------------------------------------------
      LEFT JOIN
        members AS mem
      ON 
        m.user_id = mem.id) AS m
--2) ------------------------------------------------------------------------------------------------
    LEFT JOIN
      members AS mem
    ON 
      m.mention_user_id = mem.id) AS m
--3) ---------------------------------------------------------------------------------------------------
  LEFT JOIN
    sentiment AS s
  ON
    m.tweet_id = s.tweet_id
  WHERE
    party = 'Democratic'
  AND
    mention_party = 'Democratic'
  GROUP BY
    s.sentiment,
    m.party,
    m.mention_party
    )
GROUP BY
  sentiment,
  party,
  mention_party,
  mention_total
ORDER BY
  percent DESC

-- COMMAND ----------

-- DBTITLE 1,Sentiment Statistics Democrat to Republican
SELECT
  sentiment,
  party,
  mention_party,
  CAST(FORMAT_NUMBER((mention_total/(SUM(mention_total) OVER()))*100,2) AS FLOAT) AS percent
FROM
  (SELECT
    s.sentiment,
    m.party,
    m.mention_party,
    COUNT(*) AS mention_total
  FROM
--3) This querry pulls in member information for the people mentioned.
    (SELECT
      m.user_id,
      m.tweet_id,
      m.party,
      m.mention_user_id,
      m.mention_screen_name,
      mem.party AS mention_party
    FROM
--2) The second inner most querry pulls in member information, specifically party affiliation.
      (SELECT
        m.user_id,
        m.tweet_id,
        mem.party,
        m.mention_user_id,
        m.mention_screen_name
      FROM
--1) The inner most querry is filtering out mentions not in congress or mentioned by themselves.
        (SELECT 
          m.user_id,
          m.tweet_id,
          m.mention_user_id,
          m.mention_screen_name
        FROM 
          mentions as m
        LEFT JOIN
          users as u
        ON 
          m.user_id = u.id
        WHERE 
          m.user_id <> m.mention_user_id
        AND m.mention_user_id IN (SELECT id FROM users)) AS m
--1) ------------------------------------------------------------------------------------------------
      LEFT JOIN
        members AS mem
      ON 
        m.user_id = mem.id) AS m
--2) ------------------------------------------------------------------------------------------------
    LEFT JOIN
      members AS mem
    ON 
      m.mention_user_id = mem.id) AS m
--3) ---------------------------------------------------------------------------------------------------
  LEFT JOIN
    sentiment AS s
  ON
    m.tweet_id = s.tweet_id
  WHERE
    party = 'Democratic'
  AND
    mention_party = 'Republican'
  GROUP BY
    s.sentiment,
    m.party,
    m.mention_party
    )
GROUP BY
  sentiment,
  party,
  mention_party,
  mention_total
