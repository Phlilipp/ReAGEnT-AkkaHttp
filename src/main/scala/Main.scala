/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

import Helpers.DocumentObservable
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RequestContext, RouteResult}
import com.mongodb.client.model.Filters
import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.StdIn

object Main extends CORSHandler {

  val partyMatcher = Map("CDU" -> "CDU", "SPD" -> "SPD", "FDP" -> "FDP", "Linke" -> "Linke", "B90" -> "B90", "AfD" -> "AfD", "CSU" -> "CSU", "Parteilos" -> "Parteilos")

  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "my-system")
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext: ExecutionContextExecutor = system.executionContext


    val countTotalRDD = getRDD("countTotalByYear")


    val routes = {
      concat(
        getRoutesWithPartyYearMonth("countTweetByMonth", countTotalRDD, countTotalRDD, countTotalRDD)
      )
    }


    val bindingFuture = Http().newServerAt("0.0.0.0", 8080).bind(routes)


    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }


  /**
   * Liefert eine Mongo-Collection als RDD
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
    MongoSpark.load(sc).rdd
  }


  /**
   * Liefert Paths:
   * /path
   * /path/partei
   * /path/jahr
   * /path/jahr/partei
   * /path/jahr/"week" (time.getDayOfYear / 7) + 1
   * /path/jahr/"month"
   * /path/jahr/"week"/partei
   * /path/jahr/"month"/partei
   */
  def getRoutesWithPartyYearMonth(pathName: String, collectionYear: RDD[Document], collectionMonth: RDD[Document], collectionWeek: RDD[Document]): RequestContext => Future[RouteResult] = {
    pathPrefix(pathName) {
      concat(
        pathEnd {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = collectionYear.map(elem => elem.get("_id").asInstanceOf[Document].get("party") -> (elem.get("_id").asInstanceOf[Document].get("year").toString, elem.get("count")))
                .groupBy(_._1).mapValues(_.map(_._2).toMap).collect()
              Json(DefaultFormats).write(temp)
            })))
          }
        }
        /*path(partyMatcher) { party =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionYear.find(Filters.eq("_id.party", party)).results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
          }
        },
        path(IntNumber) { year =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionYear.find(Filters.eq("_id.year", year)).results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
          }
        },
        path(IntNumber / partyMatcher) { (year, party) =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionYear.find(Filters.and(
              Filters.eq("_id.year", year),
              Filters.eq("_id.party", party))
            ).results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
          }
        },
        path(IntNumber / IntNumber) { (year, month) =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionMonth.find(Filters.and(
              Filters.eq("_id.year", year),
              Filters.eq("_id.month", month))
            ).results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
          }
        },
        path(IntNumber / IntNumber / partyMatcher) { (year, month, party) =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionMonth.find(Filters.and(
              Filters.eq("_id.year", year),
              Filters.eq("_id.month", month),
              Filters.eq("_id.party", party))
            ).results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
          }
        }*/
      )
    }
  }

  /*def main(args: Array[String]): Unit = {
    val mongoClient: MongoClient = MongoClient(sys.env("REAGENT_MONGO") + "?authSource=examples")
    val database: MongoDatabase = mongoClient.getDatabase("examples")

    val collectionProcessed: MongoCollection[Document] = database.getCollection("ProcessedTweets")
    val collectionOriginal: MongoCollection[Document] = database.getCollection("tweets_bundestag_complete")

    val collectionCountTotalByYear: MongoCollection[Document] = database.getCollection("countTotalByYear")

    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "my-system")
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext: ExecutionContextExecutor = system.executionContext


    val routes = {
      concat(
        getRoutesWithPartyYearMonth("countTweetByMonth", collectionCountTotalByYear, collectionCountTotalByYear, collectionCountTotalByYear)
      )
    }


    val bindingFuture = Http().newServerAt("0.0.0.0", 8080).bind(routes)


    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }*/
}