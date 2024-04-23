package com.evolutiongaming.cluster.sharding

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

object RegionOf {

  def apply(actorSystem: ActorSystem): ActorRef = {
    def actor(): Actor = new Actor {
      def receive: Actor.Receive = PartialFunction.empty
    }

    val props = Props(actor())
    actorSystem.actorOf(props)
  }
}
