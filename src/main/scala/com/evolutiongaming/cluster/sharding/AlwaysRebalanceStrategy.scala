package com.evolutiongaming.cluster.sharding

import akka.actor.ActorRef
import akka.cluster.sharding.ShardRegion

import scala.util.Random

object AlwaysRebalanceStrategy extends ShardingStrategy {

  def allocate(requesterRef: ActorRef, requester: Region, shard: Shard, current: Allocation) = {
    Random.shuffle(current.keys).headOption
  }

  def rebalance(current: Allocation, inProgress: Set[Shard]) = {
    if (current.size == 1) List.empty[ShardRegion.ShardId] else current.values.flatten.toList
  }
}
