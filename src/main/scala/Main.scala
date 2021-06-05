/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

import Helpers.DocumentObservable
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import com.mongodb.client.model.Filters
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn

object Main extends CORSHandler {

  def main(args: Array[String]): Unit = {
    val mongoClient: MongoClient = MongoClient(sys.env("REAGENT_MONGO") + "?authSource=examples")
    val database: MongoDatabase = mongoClient.getDatabase("examples")
    val collectionProcessed: MongoCollection[Document] = database.getCollection("ProcessedTweets")
    val collectionOriginal: MongoCollection[Document] = database.getCollection("tweets_bundestag_complete")

    val collectionCountByHashtag: MongoCollection[Document] = database.getCollection("countByHashtag")
    val collectionCountByHashtagAndParty: MongoCollection[Document] = database.getCollection("countByHashtagAndParty")
    val collectionCountByNounsAndParty: MongoCollection[Document] = database.getCollection("countByNounsAndParty")
    val collectionCountConnectedHashtags: MongoCollection[Document] = database.getCollection("countConnectedHashtags")
    val collectionCountHashtagsUsedByParty: MongoCollection[Document] = database.getCollection("countHashtagsUsedByParty")
    val collectionCountTotalByHour: MongoCollection[Document] = database.getCollection("countTotalByHour")
    val collectionCountTotalByHourAndParty: MongoCollection[Document] = database.getCollection("countTotalByHourAndParty")
    val collectionCountTotalByParty: MongoCollection[Document] = database.getCollection("countTotalByParty")
    val collectionCountTotalByYear: MongoCollection[Document] = database.getCollection("countTotalByYear")

    val collectionAvgTweetLengthByParty: MongoCollection[Document] = database.getCollection("avgTweetLengthByParty")
    val collectionAvgTweetLengthByTime: MongoCollection[Document] = database.getCollection("avgTweetLengthByTime")
    val collectionAvgTweetLengthByTimeAndParty: MongoCollection[Document] = database.getCollection("avgTweetLengthByTimeAndParty")

    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "my-system")
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext: ExecutionContextExecutor = system.executionContext

    val countByHashtag = path("countByHashtag") {
      get {
        corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountByHashtag.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countByHashtagAndParty = path("countByHashtagAndParty") {
      get {
        corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountByHashtagAndParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countByNounsAndParty = path("countByNounsAndParty") {
      get {
        corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountByNounsAndParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countConnectedHashtags = path("countConnectedHashtags") {
      get {
        corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountConnectedHashtags.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countHashtagsUsedByParty = path("countHashtagsUsedByParty") {
      get {
        corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountHashtagsUsedByParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countTotalByHour = path("countTotalByHour") {
      get {
        corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountTotalByHour.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countTotalByHourAndParty =  path("countTotalByHourAndParty") {
      parameters("year", "month") { (jahr, monat) =>
        get {
          corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountTotalByHourAndParty.find(
            Filters.and(
              Filters.eq("_id.year", jahr.toInt),
              Filters.eq("_id.month", monat.toInt)
            )
          ).results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
        }
      }
    }

    val countTotalByParty =  path("countTotalByParty") {
        get {
          corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountTotalByParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
        }
    }

    val countTotalByYear = path("countTotalByYear") {
      get {
        corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountTotalByYear.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val avgTweetLengthByParty = path("avgTweetLengthByParty") {
      get {
        corsHandler(complete(HttpEntity(ContentTypes.`application/json`,collectionAvgTweetLengthByParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val avgTweetLengthByTime = path("avgTweetLengthByTime") {
      get {
        corsHandler(complete(HttpEntity(ContentTypes.`application/json`,collectionAvgTweetLengthByTime.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val avgTweetLengthByTimeAndParty = path("avgTweetLengthByTimeAndParty"){
      get {
        corsHandler(complete(HttpEntity(ContentTypes.`application/json`,collectionAvgTweetLengthByTimeAndParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val routes = {
      concat(
        path("jsonOriginal") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionOriginal.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
          }
        },
        path("jsonProcessed") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionProcessed.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
          }
        },
        countByHashtag,
        countByHashtagAndParty,
        countByNounsAndParty,
        countConnectedHashtags,
        countHashtagsUsedByParty,
        countTotalByHour,
        countTotalByHourAndParty,
        countTotalByParty,
        countTotalByYear,
        avgTweetLengthByParty,
        avgTweetLengthByTime,
        avgTweetLengthByTimeAndParty,


        path("hello") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>")))
          }
        }
      )
    }


    val bindingFuture = Http().newServerAt("0.0.0.0", 8080).bind(routes)


    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}