package com.evolutiongaming.cluster.sharding

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

object RegionOf {

  def apply(actorSystem: ActorSystem): ActorRef = {
    def actor() = new Actor {
      def receive = PartialFunction.empty
    }

    val props = Props(actor())
    actorSystem.actorOf(props)
  }
}
