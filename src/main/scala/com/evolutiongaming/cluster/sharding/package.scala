package com.evolutiongaming.cluster

import akka.actor.ActorRef
import akka.cluster.sharding.ShardRegion

package object sharding {

  type Shard = ShardRegion.ShardId
  type Region = ActorRef
  type Allocation = Map[Region, IndexedSeq[Shard]]

  type Allocate = (Region, Shard, Allocation) => Region

  object Allocate {
    val Default: Allocate = (requester, _, _) => requester
  }
}
