/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RequestContext, Route, RouteResult}
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.toSparkContextFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import java.time.LocalDateTime
import java.util
import java.util.Calendar
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.language.postfixOps
import collection.JavaConverters._

object Main extends CORSHandler {

  val partyMatcher = Map("CDU" -> "CDU", "SPD" -> "SPD", "FDP" -> "FDP", "Linke" -> "Linke", "B90" -> "B90", "AfD" -> "AfD", "CSU" -> "CSU", "Parteilos" -> "Parteilos")

  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "my-system")
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext: ExecutionContextExecutor = system.executionContext


    val countTotalYearRDD = getRDD("countTotalByYear")
    val countTotalMonthRDD = getRDD("countTotalByMonth")
    val countTotalWeekRDD = getRDD("countTotalByWeek")

    val avgRepliesYearRDD = getRDD("avgRepliesByYear")
    val avgRepliesMonthRDD = getRDD("avgRepliesByMonth")
    val avgRepliesWeekRDD = getRDD("avgRepliesByWeek")

    val avgLikesYearRDD = getRDD("avgLikesByYear")
    val avgLikesMonthRDD = getRDD("avgLikesByMonth")
    val avgLikesWeekRDD = getRDD("avgLikesByWeek")

    val avgRetweetsYearRDD = getRDD("avgRetweetsByYear")
    val avgRetweetsMonthRDD = getRDD("avgRetweetsByMonth")
    val avgRetweetsWeekRDD = getRDD("avgRetweetsByWeek")

    val mediaUsageYearRDD = getRDD("mediaUsageByYear")
    val mediaUsageMonthRDD = getRDD("mediaUsageByMonth")
    val mediaUsageWeekRDD = getRDD("mediaUsageByWeek")


    val countHashtagsYearRDD = getRDD("hashtagsByYear")
    val countHashtagsMonthRDD = getRDD("hashtagsByMonth")
    val countHashtagsWeekRDD = getRDD("hashtagsByWeek")

    val mostTweetsWeekdayYearRDD = getRDD("mostTweetsDayByYear")
    val mostTweetsWeekdayMonthRDD = getRDD("mostTweetsDayByMonth")
    val mostTweetsWeekdayWeekRDD = getRDD("mostTweetsDayByWeek")

    val mostTweetsHourYearRDD = getRDD("mostTweetsTimeByYear")
    val mostTweetsHourMonthRDD = getRDD("mostTweetsTimeByMonth")
    val mostTweetsHourWeekRDD = getRDD("mostTweetsTimeByWeek")

    val mostActiveUsersByYearRDD = getRDD("mostActiveUsersByYear")
    val mostActiveUsersByMonthRDD = getRDD("mostActiveUsersByMonth")
    val mostActiveUsersByWeekRDD = getRDD("mostActiveUsersByWeek")

    val mostTaggedUsersByYearRDD = getRDD("mostTaggedUsersByYear")
    val mostTaggedUsersByMonthRDD = getRDD("mostTaggedUsersByMonth")
    val mostTaggedUsersByWeekRDD = getRDD("mostTaggedUsersByWeek")

    val totalRepliesByYearRDD = getRDD("totalRepliesByYear")
    val totalRepliesByMonthRDD = getRDD("totalRepliesByMonth")
    val totalRepliesByWeekRDD = getRDD("totalRepliesByWeek")


    val liveMostTweetsByHourRDD = getRDD("metricsByHourAndParty")

    val liveTweetsRDD = getRDD("lastTweets")
    //TODO Links, Durchschnittliche Tweetlänge

    val routes = {
      concat(
        getRoutesWithCount("countTweetByMonth", countTotalYearRDD, countTotalMonthRDD, countTotalWeekRDD),
        getRoutesWithCount("averageReply", avgRepliesYearRDD, avgRepliesMonthRDD, avgRepliesWeekRDD),
        getRoutesWithCount("averagelikestweet", avgLikesYearRDD, avgLikesMonthRDD, avgLikesWeekRDD),
        getRoutesWithCount("averageRetweetsTweet", avgRetweetsYearRDD, avgRetweetsMonthRDD, avgRetweetsWeekRDD),
        getRoutesWithCount("mediausagetweets", mediaUsageYearRDD, mediaUsageMonthRDD, mediaUsageWeekRDD),
        getRoutesWithCount("totalReplies", totalRepliesByYearRDD, totalRepliesByMonthRDD, totalRepliesByWeekRDD),
        getRoutesWithStrings("mostUsedHashtags", countHashtagsYearRDD, countHashtagsMonthRDD, countHashtagsWeekRDD, 5, "hashtag"),
        getRoutesWithStringsWithoutRanking("mostUsedHashtagsCloud", countHashtagsYearRDD, countHashtagsMonthRDD, countHashtagsWeekRDD, 100, "hashtag"),
        getRoutesWithStringsWithoutRanking("mosttweetsday", mostTweetsWeekdayYearRDD, mostTweetsWeekdayMonthRDD, mostTweetsWeekdayWeekRDD, 7, "weekday"),
        getRoutesWithStringsWithoutRanking("mosttweetstime", mostTweetsHourYearRDD, mostTweetsHourMonthRDD, mostTweetsHourWeekRDD, 24, "hour"),
        getRoutesWithStrings("mostActiveUser", mostActiveUsersByYearRDD, mostActiveUsersByMonthRDD, mostActiveUsersByWeekRDD, 10, "user"),
        getRoutesWithStrings("mostTaggedUser", mostTaggedUsersByYearRDD, mostTaggedUsersByMonthRDD, mostTaggedUsersByWeekRDD, 10, "taggedUser"),
        getLiveRoutesWithCount("countTotalRunning", liveMostTweetsByHourRDD),
        getLiveTweetsRoute("liveTweets", liveTweetsRDD)
      )
    }

    Http().newServerAt("0.0.0.0", 8080).bind(routes)
    //    val bindingFuture = Http().newServerAt("0.0.0.0", 8080).bind(routes)


    //println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    //StdIn.readLine() // let it run until user presses return
    //bindingFuture
    //.flatMap(_.unbind()) // trigger unbinding from the port
    //.onComplete(_ => system.terminate()) // and shutdown when done
  }


  /**
   * Liefert eine Mongo-Collection als RDD
   *
   * @param collectionName Name der Mongo-Collection
   * @return RDD[Document]
   */
  def getRDD(collectionName: String): RDD[Document] = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnector")
      .config("spark.mongodb.input.uri", sys.env("REAGENT_MONGO") + "examples." + collectionName + "?authSource=examples")
      .config("spark.testing.memory", 2147480000)
      .getOrCreate()

    val sc = sparkSession.sparkContext
    sc.loadFromMongoDB(ReadConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples." + collectionName + "?authSource=examples")))).rdd
  }


  /**
   * Liefert Paths:
   * /path -> [{"CDU" : {"2017": 12234, "2018": 63578, ...}}, {"SPD": {...}}, ...]
   * /path/partei -> {"2017": 12234, "2018": 63578, ...}
   * /path/jahr/"week" -> [{"CDU" : {"01": 124, "02": 642, ...}}, {"SPD": {...}}, ...]
   * /path/jahr/"month" -> [{"CDU" : {"01": 1240, "02": 2425, ...}}, {"SPD": {...}}, ...]
   * /path/jahr/"week"/partei -> {"01": 124, "02": 642, ...}
   * /path/jahr/"month"/partei -> {"01": 1240, "02": 2425, ...}
   */
  def getRoutesWithCount(pathName: String, collectionYear: RDD[Document], collectionMonth: RDD[Document], collectionWeek: RDD[Document], timespan: String = ""): RequestContext => Future[RouteResult] = {

    var yearString = "year"
    var monthString = "month"
    var weekString = "week"

    if (timespan != "") {
      yearString = timespan
      monthString = timespan
      weekString = timespan
    }

    pathPrefix(pathName) {
      concat(
        pathEnd {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionYear
                .map(elem => elem.get("_id").asInstanceOf[Document].get("party").toString -> (elem.get("_id").asInstanceOf[Document].get("year").toString, elem.get("count")))
                .groupBy(_._1)
                .mapValues(_.map(_._2).toMap)
                .collect()
                .sortBy(_._1)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(partyMatcher) { party =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionYear
                .filter(_.get("_id").asInstanceOf[Document].getString("party") == party)
                .map(elem => (elem.get("_id").asInstanceOf[Document].get("year").toString, elem.get("count").toString.toDouble))
                .collect()
                .toMap
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(IntNumber / "month") { year =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionMonth
                .filter(_.get("_id").asInstanceOf[Document].getInteger("year") == year)
                .groupBy(_.get("_id").asInstanceOf[Document].get("party").toString)
                .mapValues(_.map(elem => (elem.get("_id").asInstanceOf[Document].getInteger("month").formatted("%02d"), elem.get("count").toString.toDouble)).toMap)
                .collect()
                .sortBy(_._1)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(IntNumber / "week") { year =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionWeek
                .filter(_.get("_id").asInstanceOf[Document].getInteger("year") == year)
                .groupBy(_.get("_id").asInstanceOf[Document].get("party").toString)
                .mapValues(_.map(elem => (elem.get("_id").asInstanceOf[Document].getInteger("week").formatted("%02d"), elem.get("count").toString.toDouble)).toMap)
                .collect()
                .sortBy(_._1)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(IntNumber / "month" / partyMatcher) { (year, party) =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionMonth
                .filter(_.get("_id").asInstanceOf[Document].getInteger("year") == year)
                .filter(_.get("_id").asInstanceOf[Document].getString("party") == party)
                .map(elem => (elem.get("_id").asInstanceOf[Document].getInteger("month").formatted("%02d"), elem.get("count").toString.toDouble))
                .collect()
                .toMap
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(IntNumber / "week" / partyMatcher) { (year, party) =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionWeek
                .filter(_.get("_id").asInstanceOf[Document].getInteger("year") == year)
                .filter(_.get("_id").asInstanceOf[Document].getString("party") == party)
                .map(elem => (elem.get("_id").asInstanceOf[Document].getInteger("week").formatted("%02d"), elem.get("count").toString.toDouble))
                .collect()
                .toMap
              Json(DefaultFormats).write(temp)
            })))
          }
        }
      )
    }
  }

  /**
   * Liefert Paths:
   * /path -> [{"CDU" : {"2017": {"1": {"String1": 45223}, ... "max": {"StringMax": 42533}}, "2018": {...}, ...}, {"SPD": {...}}, ...]
   * /path/partei -> {"2017": {"1": {"String1": 45223}, ... "max": {"StringMax": 42533}}, "2018": {...}, ...}
   * /path/jahr/"week" -> [{"CDU" : {"01": {"1": {"String1": 45223}, ... "max": {"StringMax": 42533}}, "02": {...}, ...}, {"SPD": {...}}, ...]
   * /path/jahr/"month" -> [{"CDU" : {"01": {"1": {"String1": 45223}, ... "max": {"StringMax": 42533}}, "02": {...}, ...}, {"SPD": {...}}, ...]
   * /path/jahr/"week"/partei -> {"01": {"1": {"String1": 45223}, ... "max": {"StringMax": 42533}}, "02": {...}, ...}
   * /path/jahr/"month"/partei -> {"01": {"1": {"String1": 45223}, ... "max": {"StringMax": 42533}}, "02": {...}, ...}
   */
  def getRoutesWithStrings(pathName: String, collectionYear: RDD[Document], collectionMonth: RDD[Document], collectionWeek: RDD[Document], takeCount: Int, searchString: String): RequestContext => Future[RouteResult] = {

    pathPrefix(pathName) {
      concat(
        pathEnd {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionYear
                .map(elem => elem.get("_id").asInstanceOf[Document].get("party") ->
                  (elem.get("_id").asInstanceOf[Document].get("year").toString,
                    elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                    elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2._1).mapValues(_.groupBy(_._2._2).mapValues(_.map(_._2._3.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).zipWithIndex.map(elem => ((elem._2 + 1).toString, (elem._1._1, elem._1._2))).toMap))
                .toList
                .sortBy(_._1.toString)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(partyMatcher) { party =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionYear
                .filter(_.get("_id").asInstanceOf[Document].getString("party") == party)
                .map(elem => (elem.get("_id").asInstanceOf[Document].get("year").toString,
                  elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                  elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2).mapValues(_.map(_._3.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).zipWithIndex.map(elem => ((elem._2 + 1).toString, (elem._1._1, elem._1._2))).toMap)
                .toList
                .sortBy(_._1)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(IntNumber / "month") { year =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionMonth
                .filter(_.get("_id").asInstanceOf[Document].getInteger("year") == year)
                .map(elem => elem.get("_id").asInstanceOf[Document].get("party") ->
                  (elem.get("_id").asInstanceOf[Document].get("month").toString,
                    elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                    elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2._1).mapValues(_.groupBy(_._2._2).mapValues(_.map(_._2._3.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).zipWithIndex.map(elem => ((elem._2 + 1).toString, (elem._1._1, elem._1._2))).toMap))
                .toList
                .sortBy(_._1.toString)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(IntNumber / "week") { year =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionWeek
                .filter(_.get("_id").asInstanceOf[Document].getInteger("year") == year)
                .map(elem => elem.get("_id").asInstanceOf[Document].get("party") ->
                  (elem.get("_id").asInstanceOf[Document].get("week").toString,
                    elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                    elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2._1).mapValues(_.groupBy(_._2._2).mapValues(_.map(_._2._3.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).zipWithIndex.map(elem => ((elem._2 + 1).toString, (elem._1._1, elem._1._2))).toMap))
                .toList
                .sortBy(_._1.toString)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(IntNumber / "month" / partyMatcher) { (year, party) =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionMonth
                .filter(_.get("_id").asInstanceOf[Document].getInteger("year") == year)
                .filter(_.get("_id").asInstanceOf[Document].getString("party") == party)
                .map(elem => (elem.get("_id").asInstanceOf[Document].get("month").toString,
                  elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                  elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2).mapValues(_.map(_._3.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).zipWithIndex.map(elem => ((elem._2 + 1).toString, (elem._1._1, elem._1._2))).toMap)
                .toList
                .sortBy(_._1.toLong)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(IntNumber / "week" / partyMatcher) { (year, party) =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionWeek
                .filter(_.get("_id").asInstanceOf[Document].getInteger("year") == year)
                .filter(_.get("_id").asInstanceOf[Document].getString("party") == party)
                .map(elem => (elem.get("_id").asInstanceOf[Document].get("week").toString,
                  elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                  elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2).mapValues(_.map(_._3.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).zipWithIndex.map(elem => ((elem._2 + 1).toString, (elem._1._1, elem._1._2))).toMap)
                .toList
                .sortBy(_._1.toLong)
              Json(DefaultFormats).write(temp)
            })))
          }
        }
      )
    }
  }

  /**
   * Liefert Paths:
   * /path -> [{"CDU" : {"2017": {"String1": 5423, ... "StringMax":24352}, "2018": {...}, ...}, {"SPD": {...}}, ...]
   * /path/partei -> {"2017": {"String1": 5423, ... "StringMax":24352}, "2018": {...}, ...}
   * /path/jahr/"week" -> [{"CDU" : {"01": {"String1": 5423, ... "StringMax":24352}, "02": {...}, ...}, {"SPD": {...}}, ...]
   * /path/jahr/"month" -> [{"CDU" : {"01": {"String1": 5423, ... "StringMax":24352}, "02": {...}, ...}, {"SPD": {...}}, ...]
   * /path/jahr/"week"/partei -> {"01": {"String1": 5423, ... "StringMax":24352}, "02": {...}, ...}
   * /path/jahr/"month"/partei -> {"01": {"String1": 5423, ... "StringMax":24352}, "02": {...}, ...}
   */
  def getRoutesWithStringsWithoutRanking(pathName: String, collectionYear: RDD[Document], collectionMonth: RDD[Document], collectionWeek: RDD[Document], takeCount: Int, searchString: String): RequestContext => Future[RouteResult] = {

    pathPrefix(pathName) {
      concat(
        pathEnd {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionYear
                .map(elem => elem.get("_id").asInstanceOf[Document].get("party") ->
                  (elem.get("_id").asInstanceOf[Document].get("year").toString,
                    elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                    elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2._1).mapValues(_.groupBy(_._2._2).mapValues(_.map(_._2._3.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).toMap))
                .toList
                .sortBy(_._1.toString)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path("legislatur") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionYear
                .map(elem => elem.get("_id").asInstanceOf[Document].get("party") ->
                  (elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                    elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2._1).mapValues(_.map(_._2._2.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).toMap)
                .toList
                .sortBy(_._1.toString)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(partyMatcher) { party =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionYear
                .filter(_.get("_id").asInstanceOf[Document].getString("party") == party)
                .map(elem => (elem.get("_id").asInstanceOf[Document].get("year").toString,
                  elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                  elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2).mapValues(_.map(_._3.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).toMap)
                .toList
                .sortBy(_._1)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(IntNumber / "month") { year =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionMonth
                .filter(_.get("_id").asInstanceOf[Document].getInteger("year") == year)
                .map(elem => elem.get("_id").asInstanceOf[Document].get("party") ->
                  (elem.get("_id").asInstanceOf[Document].get("month").toString,
                    elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                    elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2._1).mapValues(_.groupBy(_._2._2).mapValues(_.map(_._2._3.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).toMap))
                .toList
                .sortBy(_._1.toString)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(IntNumber / "week") { year =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionWeek
                .filter(_.get("_id").asInstanceOf[Document].getInteger("year") == year)
                .map(elem => elem.get("_id").asInstanceOf[Document].get("party") ->
                  (elem.get("_id").asInstanceOf[Document].get("week").toString,
                    elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                    elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2._1).mapValues(_.groupBy(_._2._2).mapValues(_.map(_._2._3.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).toMap))
                .toList
                .sortBy(_._1.toString)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(IntNumber / "month" / partyMatcher) { (year, party) =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionMonth
                .filter(_.get("_id").asInstanceOf[Document].getInteger("year") == year)
                .filter(_.get("_id").asInstanceOf[Document].getString("party") == party)
                .map(elem => (elem.get("_id").asInstanceOf[Document].get("month").toString,
                  elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                  elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2).mapValues(_.map(_._3.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).toMap)
                .toList
                .sortBy(_._1.toLong)
              Json(DefaultFormats).write(temp)
            })))
          }
        },
        path(IntNumber / "week" / partyMatcher) { (year, party) =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionWeek
                .filter(_.get("_id").asInstanceOf[Document].getInteger("year") == year)
                .filter(_.get("_id").asInstanceOf[Document].getString("party") == party)
                .map(elem => (elem.get("_id").asInstanceOf[Document].get("week").toString,
                  elem.get("_id").asInstanceOf[Document].get(searchString).toString,
                  elem.get("count")))
                .groupBy(_._1)
                .collect()
                .toMap
                .mapValues(_.groupBy(_._2).mapValues(_.map(_._3.toString.toDouble).sum).toList.sortBy(-_._2).take(takeCount).toMap)
                .toList
                .sortBy(_._1.toLong)
              Json(DefaultFormats).write(temp)
            })))
          }
        }
      )
    }
  }


  /**
   * Liefert Paths:
   * /path/day -> [{"CDU" : {"1": 4, "2": 1, ...}, {"SPD": {...}}, ...]
   * /path/day/partei -> {"1": 4, "2": 1, ...}
   * /path/month -> [{"CDU" : {"1": 43, "2": 64, ...}, {"SPD": {...}}, ...]
   * /path/month/partei -> {"1": 43, "2": 64, ...}
   */
  def getLiveRoutesWithCount(pathName: String, collectionHour: RDD[Document]): RequestContext => Future[RouteResult] = {

    val now = LocalDateTime.now()
    val today = (now.getYear.toString, now.getMonth.getValue.toString, now.getDayOfMonth.toString)
    val currentMonth = (now.getYear.toString, now.getMonth.getValue.toString)

    val calendar = Calendar.getInstance()
    calendar.set(now.getYear, now.getMonth.getValue - 1, now.getDayOfMonth)
    val daysInMonth = calendar.getActualMaximum(Calendar.DAY_OF_MONTH)

    concat(
      path(pathName / "day") {
        get {
          corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
            val temp = collectionHour
              .map(elem => elem.get("_id").asInstanceOf[Document].get("party") ->
                (elem.get("_id").asInstanceOf[Document].get("year").toString,
                  elem.get("_id").asInstanceOf[Document].get("month").toString,
                  elem.get("_id").asInstanceOf[Document].get("day").toString,
                  elem.get("_id").asInstanceOf[Document].get("hour").toString,
                  elem.get("count")))
              .filter(elem => (elem._2._1, elem._2._2, elem._2._3) == today)
              .groupBy(_._1)
              .collect()
              .toMap
              .mapValues(elem => addZeros(elem.groupBy(_._2._4).mapValues(_.map(_._2._5.toString.toDouble).sum), 24))
              .toList
              .sortBy(_._1.toString)
            Json(DefaultFormats).write(temp)
          })))
        }
      },
      path(pathName / "day" / partyMatcher) { party =>
        get {
          corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
            val temp = collectionHour
              .map(elem => elem.get("_id").asInstanceOf[Document].get("party") ->
                (elem.get("_id").asInstanceOf[Document].get("year").toString,
                  elem.get("_id").asInstanceOf[Document].get("month").toString,
                  elem.get("_id").asInstanceOf[Document].get("day").toString,
                  elem.get("_id").asInstanceOf[Document].get("hour").toString,
                  elem.get("count")))
              .filter(elem => (elem._2._1, elem._2._2, elem._2._3) == today)
              .groupBy(_._1)
              .filter(_._1 == party)
              .collect()
              .toMap
              .mapValues(elem => addZeros(elem.groupBy(_._2._4).mapValues(_.map(_._2._5.toString.toDouble).sum), 24))
              .toList
              .head._2
            Json(DefaultFormats).write(temp)
          })))
        }
      },
      path(pathName / "month") {
        get {
          corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
            val temp = collectionHour
              .map(elem => elem.get("_id").asInstanceOf[Document].get("party") ->
                (elem.get("_id").asInstanceOf[Document].get("year").toString,
                  elem.get("_id").asInstanceOf[Document].get("month").toString,
                  elem.get("_id").asInstanceOf[Document].get("day").toString,
                  elem.get("count")))
              .filter(elem => (elem._2._1, elem._2._2) == currentMonth)
              .groupBy(_._1)
              .collect()
              .toMap
              .mapValues(elem => addZeros(elem.groupBy(_._2._3).mapValues(_.map(_._2._4.toString.toDouble).sum), daysInMonth, 1))
              .toList
              .sortBy(_._1.toString)
            Json(DefaultFormats).write(temp)
          })))
        }
      },
      path(pathName / "month" / partyMatcher) { party =>
        get {
          corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
            val temp = collectionHour
              .map(elem => elem.get("_id").asInstanceOf[Document].get("party") ->
                (elem.get("_id").asInstanceOf[Document].get("year").toString,
                  elem.get("_id").asInstanceOf[Document].get("month").toString,
                  elem.get("_id").asInstanceOf[Document].get("day").toString,
                  elem.get("count")))
              .filter(elem => (elem._2._1, elem._2._2) == currentMonth)
              .groupBy(_._1)
              .filter(_._1 == party)
              .collect()
              .toMap
              .mapValues(elem => addZeros(elem.groupBy(_._2._3).mapValues(_.map(_._2._4.toString.toDouble).sum), daysInMonth, 1))
              .toList
              .head._2
            Json(DefaultFormats).write(temp)
          })))
        }
      }
    )
  }

  /**
   * Liefert Paths:
   * /path -> [{"CDU" : {"1": 439457823409783, "2": 43908723948172, ...}, {"SPD": {...}}, ...]
   * /path/partei -> {"1": 439457823409783, "2": 43908723948172, ...}
   */
  def getLiveTweetsRoute(pathName: String, collection: RDD[Document]): RequestContext => Future[RouteResult] = {
    concat(
      path(pathName) {
        get {
          corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
            val temp = collection
              .map(elem => (elem.getString("_id"), elem.get("tweetIds").asInstanceOf[util.ArrayList[String]].asScala.toList))
              .mapValues(_.zipWithIndex.map(elem => (elem._2 + 1, elem._1)).toMap)
              .collect()
              .sortBy(_._1)
            Json(DefaultFormats).write(temp)
          })))
        }
      },
      path(pathName / partyMatcher) { party =>
        get {
          corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
            val temp = collection
              .map(elem => (elem.getString("_id"), elem.get("tweetIds").asInstanceOf[util.ArrayList[String]].asScala.toList))
              .mapValues(_.zipWithIndex.map(elem => (elem._2 + 1, elem._1)).toMap)
              .collect()
              .toList
              .filter(_._1 == party)
              .head
            Json(DefaultFormats).write(temp)
          })))
        }
      }
    )
  }

  def addZeros(map: Map[String, Double], size: Int, offset: Int = 0): Map[String, Double] = {
    val b = Array.fill(size)(0.0).zipWithIndex.map(elem => ((elem._2 + offset).toString, elem._1)).toList
    map.toList.union(b).groupBy(_._1).mapValues(_.map(_._2).sum)
  }
}