package com.evolutiongaming.cluster.sharding

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait ActorSpec extends BeforeAndAfterAll { this: Suite =>

  implicit lazy val system: ActorSystem = ActorSystem(getClass.getSimpleName)

  override protected def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

  abstract class ActorScope extends TestKit(system) with ImplicitSender with DefaultTimeout
}
