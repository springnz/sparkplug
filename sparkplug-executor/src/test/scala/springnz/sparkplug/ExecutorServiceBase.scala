package springnz.sparkplug

import akka.actor._
import akka.testkit.TestKitBase
import springnz.sparkplug.core.{ Configurer, LocalConfigurer }
import springnz.sparkplug.executor.ExecutorService
import springnz.sparkplug.executor.MessageTypes._

trait ExecutorServiceBase { self: TestKitBase ⇒

  class ExecutorServiceFixture(probe: ActorRef, clientName: String, brokerName: String) {

    val executorService = new ExecutorService("TestService", brokerName) {
      // Run it locally in Spark
      override val configurer: Configurer = new LocalConfigurer("TestService", None)
    }

    executorService.start(system, s"/user/$clientName")

    val clientActor = system.actorOf(Props(new Actor {
      override def receive = {
        case ServerReady ⇒
          probe forward ServerReady
          sender ! ClientReady
      }
    }), clientName)
  }

}
