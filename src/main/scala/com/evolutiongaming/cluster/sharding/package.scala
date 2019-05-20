package com.evolutiongaming.cluster

import akka.actor.{ActorRef, Address}
import akka.cluster.sharding.ShardRegion

import scala.collection.immutable.IndexedSeq

package object sharding {

  type Shard = ShardRegion.ShardId
  type Region = ActorRef
  type Allocation = Map[Region, IndexedSeq[Shard]]

  type AddressOf = Region => Address

  type Allocate = (Region, Shard, Allocation) => Region

  object Allocate {
    val Default: Allocate = (requester, _, _) => requester
  }
}
