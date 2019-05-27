package com.evolutiongaming.cluster.sharding

import akka.actor.{Actor, Address, Props}
import org.scalatest.{FunSuite, Matchers}

class AbsoluteAddressSpec extends FunSuite with ActorSpec with Matchers {

  test("AbsoluteAddress") {
    
    def actor() = new Actor {
      def receive = PartialFunction.empty
    }

    val props = Props(actor())
    val ref = actorSystem.actorOf(props)
    AbsoluteAddress(actorSystem).apply(ref.path.address) shouldEqual Address("akka", "AbsoluteAddressSpec")
  }
}