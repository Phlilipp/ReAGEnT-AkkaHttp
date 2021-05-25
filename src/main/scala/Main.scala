/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

import Helpers.DocumentObservable
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.{DELETE, GET, OPTIONS, POST, PUT}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{`Access-Control-Allow-Credentials`, `Access-Control-Allow-Headers`, `Access-Control-Allow-Methods`, `Access-Control-Allow-Origin`}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route}
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn

object Main {

  def main(args: Array[String]): Unit = {
    val mongoClient: MongoClient = MongoClient("mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/?authSource=examples")
    val database: MongoDatabase = mongoClient.getDatabase("examples")
    val collectionProcessed: MongoCollection[Document] = database.getCollection("ProcessedTweets")
    val collectionOriginal: MongoCollection[Document] = database.getCollection("SampleData")

    val collectionCountByHashtag: MongoCollection[Document] = database.getCollection("countByHashtag")
    val collectionCountByHashtagAndParty: MongoCollection[Document] = database.getCollection("countByHashtagAndParty")
    val collectionCountByNounsAndParty: MongoCollection[Document] = database.getCollection("countByNounsAndParty")
    val collectionCountConnectedHashtags: MongoCollection[Document] = database.getCollection("countConnectedHashtags")
    val collectionCountHashtagsUsedByParty: MongoCollection[Document] = database.getCollection("countHashtagsUsedByParty")
    val collectionCountTotalByHour: MongoCollection[Document] = database.getCollection("countTotalByHour")
    val collectionCountTotalByHourAndParty: MongoCollection[Document] = database.getCollection("countTotalByHourAndParty")
    val collectionCountTotalByParty: MongoCollection[Document] = database.getCollection("countTotalByParty")

    val collectionAvgTweetLengthByParty: MongoCollection[Document] = database.getCollection("avgTweetLengthByParty")
    val collectionAvgTweetLengthByTime: MongoCollection[Document] = database.getCollection("avgTweetLengthByTime")
    val collectionAvgTweetLengthByTimeAndParty: MongoCollection[Document] = database.getCollection("avgTweetLengthByTimeAndParty")

    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "my-system")
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext: ExecutionContextExecutor = system.executionContext

    val ch: CORSHandler = new CORSHandler {}

    val countByHashtag = path("countByHashtag") {
      get {
        ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountByHashtag.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countByHashtagAndParty = path("countByHashtagAndParty") {
      get {
        ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountByHashtagAndParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countByNounsAndParty = path("countByNounsAndParty") {
      get {
        ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountByNounsAndParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countConnectedHashtags = path("countConnectedHashtags") {
      get {
        ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountConnectedHashtags.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countHashtagsUsedByParty = path("countHashtagsUsedByParty") {
      get {
        ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountHashtagsUsedByParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countTotalByHour = path("countTotalByHour") {
      get {
        ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountTotalByHour.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countTotalByHourAndParty =  path("countTotalByHourAndParty") {
      get {
        ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountTotalByHourAndParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val countTotalByParty =  path("countTotalByParty") {
      get {
        ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionCountTotalByParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val avgTweetLengthByParty = path("avgTweetLengthByParty") {
      get {
        ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`,collectionAvgTweetLengthByParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val avgTweetLengthByTime = path("avgTweetLengthByTime") {
      get {
        ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`,collectionAvgTweetLengthByTime.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val avgTweetLengthByTimeAndParty = path("avgTweetLengthByTimeAndParty"){
      get {
        ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`,collectionAvgTweetLengthByTimeAndParty.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
      }
    }

    val routes = {
      concat(
        path("jsonOriginal") {
          get {
            ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionOriginal.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
          }
        },
        path("jsonProcessed") {
          get {
            ch.corsHandler(complete(HttpEntity(ContentTypes.`application/json`, collectionProcessed.find().results().map(_.toJson()).toArray.mkString("[", ",", "]"))))
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
        avgTweetLengthByParty,
        avgTweetLengthByTime,
        avgTweetLengthByTimeAndParty,

        path("hello") {
          get {
            ch.corsHandler(complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>")))
          }
        }
      )
    }


    val bindingFuture = Http().newServerAt("localhost", 8080).bind(routes)


    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}