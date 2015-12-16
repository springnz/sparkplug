package springnz.sparkplug

import akka.actor._
import akka.testkit.TestKitBase
import springnz.sparkplug.core.{ Configurer, LocalConfigurer }
import springnz.sparkplug.executor.ExecutorService
import springnz.sparkplug.executor.MessageTypes._

import scala.concurrent.Promise

trait ExecutorServiceBase { self: TestKitBase ⇒

  class ExecutorServiceFixture(probe: ActorRef, clientName: String, brokerName: String) {

    val readyPromise = Promise[Unit]

    val executorService = new ExecutorService("TestService", brokerName) {
      // Run it locally in Spark
      override val configurer: Configurer = new LocalConfigurer("TestService", None)
    }

    val clientActor = system.actorOf(Props(new Actor {
      override def receive = {
        case ServerReady ⇒
          probe forward ServerReady
          sender ! ClientReady
          readyPromise.success(())
      }
    }), clientName)

    executorService.start(system, s"/user/$clientName")
  }

}
