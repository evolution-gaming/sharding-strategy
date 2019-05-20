package com.evolutiongaming.cluster.sharding

import akka.actor.ActorRef
import akka.cluster.sharding.ShardRegion

import scala.util.Random

object AlwaysRebalanceStrategy {

  def apply(random: Random = new Random()): ShardingStrategy = new ShardingStrategy {

    def allocate(requester: ActorRef, shard: Shard, current: Allocation) = {
      random.shuffle(current.keys).headOption
    }

    def rebalance(current: Allocation, inProgress: Set[Shard]) = {
      if (current.size == 1) List.empty[ShardRegion.ShardId] else current.values.flatten.toList
    }
  }
}
