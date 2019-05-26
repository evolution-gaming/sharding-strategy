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


  def const(address: Address): AddressOf = new AddressOf {
    def apply(region: Region) = address
  }
}
