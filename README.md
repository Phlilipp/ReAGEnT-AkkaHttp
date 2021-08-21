# ReAGEnT-AkkaHttp


### Routes used for specifying the data
Every route consists of a base route and is then combined with another route where the data that will be returned can be specified.

An example usage is:
+ "/mostUsedHashtags/2021/month/CSU"

This will only return data of the party 'CSU' in the year '2021' from the 'mostUsedHashtags' base route.

Those routes can be appended to the base routes to specify the returned data. As parameters 'jahr' and 'partei' are available:

Available 'partei' parameters to specify the party:
+ CDU
+ CSU
+ SPD
+ FDP
+ Linke
+ B90
+ AfD
+ Parteilos

Available 'jahr' parameters tp specify the year:
+ 2017
+ 2018
+ 2019
+ 2020
+ 2021

### Routes that can be added to the Twint data routes
| Method | Route                               |Parameter |Description            |
| :-----:|:------------------------------------|:----------------- |:--------------------- |
| `GET` | `/{jahr}/week`| `jahr` : `Int ` | only data of the specified year will be returned divided into weeks |
| `GET` | `/{jahr}/month`| `jahr` : `Int` | only data of the specified year will be returned divided into months |
| `GET` | `/{partei}`| `partei` : `String`  | only data of the specified party will be returned |
| `GET` | `/{jahr}/week/{partei}`| `jahr` : `Int` <br> `partei` : `String` | only data of the specified party and year will be returned divided into weeks |
| `GET` | `/{jahr}/month/{partei}`| `jahr` : `Int` <br> `partei` : `String` |only data of the specified party and year will be returned divided into months|

### Routes that can be added to the Live data routes (except LiveTweets)
| Method | Route                               |Parameter |Description            |
| :-----:|:------------------------------------|:----------------- |:--------------------- |
| `GET` | `/day`| - | only data of the current day will be returned |
| `GET` | `/day/{partei}`| `partei` : `String` | only data of the specified party will be returned for the current day  |
| `GET` | `/month`|  -  | only data of the current month will be returned divided into days |
| `GET` | `/month/{partei}`| `partei` : `String` | only data of the specified party and the current month will be returned divided into days|

The 15 newest Tweets can be requested at /liveTweets. And the 15 newest Tweets of one Party at /liveTweets/{partei}


## Available base routes for Twint data
The following base routes return historic data extracted from Twint.

| Method | Route                               | Description            |
| :-----:|:------------------------------------| :--------------------- |
| `GET` | `/countTweetByMonth`|Gets the tweet Count by Month|
| `GET` | `/averagelikesTweet`|Gets the average likes per tweet count by month|
| `GET` | `/averageTweetLength`|Gets the average tweet length by Month|
| `GET` | `/mostUsedHashtags`|Gets the most used hashtags by month|
| `GET` | `/mosttweetstime`|Gets the most tweets per hour|
| `GET` | `/mediausagetweets`|Returns the percentage of tweets, which contain media (pictures, videos, etc.)|
| `GET` | `/averageReply`|Gets the average reply count per tweet|
| `GET` | `/averageRetweetsTweet`|Gets the average retweet count per tweet|
| `GET` | `/mostUsedUrls`|Gets the 10 most used urls used in tweets|
| `GET` | `/mosttweetsday`|Gets the tweet Count by Month|
| `GET` | `/totalReplies`|Gets the total count of replies to tweets|
| `GET` | `/mostTaggedUser`|Gets the 10 most tagged users|
| `GET` | `/mostActiveUser`|Gets the 10 users with the most tweets|


## Available base routes for Live data
The following base routes return live data of the current day extracted via a Twitter stream

| Method | Route                               | Description            |
| :-----:|:------------------------------------| :--------------------- |
| `GET` | `/countTotalRunning`|Returns the number of tweets |
| `GET` | `/liveMediaUsage`|Returns the percentage of tweets, which contain media (pictures, videos, etc.) |
| `GET` | `/liveSentiment`|Returns the live sentiment |
| `GET` | `/liveMostactiveUser`| Returns the 10 most active users |
| `GET` | `/liveSource`|Returns the sources that the most data was sent from |
| `GET` | `/liveHashtags`|Returns the 10 most used hashtags |
| `GET` | `/liveTweets`| Returns the 15 latest tweets |


## Build

To create an executable jar bundled with all dependencies, we use sbt-assembly. It is defined at project/plugins.sbt.

To build a .jar file, run sbt clean assembly in the project directory.
The generated jar will be located in target/scala-<SCALA_VERSION>/ReAGEnT-AkkaHttp-assembly-0.1.jar.

## Local startup

Execute the jar with `java -jar ReAGEnT-AkkaHttp-assembly-0.1.jar`\
You can access the routes above under localhost:8080/{route}.
