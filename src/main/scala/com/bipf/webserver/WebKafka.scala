package com.bipf.webserver

import akka.actor.{ Actor, ActorSystem, Props, ActorLogging }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import spray.json.DefaultJsonProtocol._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.StdIn
import com.bipf.kafka.KafkaProducerClient
import org.apache.kafka.clients.producer.ProducerRecord

object WebKafka {

  case class Event(topic: String, message: String)

  class KafkaSinkActor extends Actor with ActorLogging {
    val kafkaSink = new KafkaProducerClient()
    def receive = {
      case event @ Event(topic, message) =>
        val data = new ProducerRecord[String, String](topic, message)
        kafkaSink.send(data)
        log.info(s"receive : $topic, $message")
      case _ => log.info("Invalid message")
    }

    override def postStop = {
      kafkaSink.close()
    }
    
  }

  // these are from spray-json
  implicit val bidFormat = jsonFormat2(Event)

  def main(args: Array[String]) {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val kafkaProduer = system.actorOf(Props[KafkaSinkActor], "kafka")

    val route =
      path("kafka") {
        put {
          //localhost:8080/kafka?topic=mytest&message=mymessage
          parameter("topic", "message") { (topic, message) =>
            // place a bid, fire-and-forget
            kafkaProduer ! Event(topic, message)
            complete((StatusCodes.Accepted, "message pushed to kafka"))
          }
        }
      }

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done

  }

}