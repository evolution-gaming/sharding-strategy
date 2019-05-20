package com.evolutiongaming.cluster.sharding

import akka.actor.{ActorSystem, Address}

trait AddressOf {
  def apply(region: Region): Address
}

object AddressOf {

  def apply(actorSystem: ActorSystem): AddressOf = {
    val absoluteAddress = AbsoluteAddress(actorSystem)
    region: Region => absoluteAddress(region.path.address)
  }
}
