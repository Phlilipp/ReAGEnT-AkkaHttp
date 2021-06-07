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
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.StdIn

object Main extends CORSHandler {

  val partyMatcher = Map("cdu" -> "CDU", "spd" -> "SPD", "fdp" -> "FDP", "linke" -> "Linke", "gruene" -> "B90", "afd" -> "AfD", "csu" -> "CSU", "parteilos" -> "Parteilos")

  def main(args: Array[String]): Unit = {
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
        getRoutesWithPartyYearMonth("countTweetByMonth", collectionCountTotalByYear, collectionCountTotalByYear)
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
   * Liefert Paths:
   * /path
   * /path/partei
   * /path/jahr
   * /path/jahr/partei
   * /path/jahr/monat
   * /path/jahr/monat/partei
   */
  def getRoutesWithPartyYearMonth(pathName: String, collectionYear: MongoCollection[Document], collectionMonth: MongoCollection[Document]): RequestContext => Future[RouteResult] = {
    pathPrefix(pathName) {
      concat(
        pathEnd {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionYear.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
          }
        },
        path(partyMatcher) { party =>
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
        }
      )
    }
  }
}