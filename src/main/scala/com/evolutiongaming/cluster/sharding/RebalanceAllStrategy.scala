package com.evolutiongaming.cluster.sharding

import akka.actor.ActorRef

object RebalanceAllStrategy extends ShardingStrategy {

  def allocate(requesterRef: ActorRef, requester: Region, shard: Shard, current: Allocation) = None

  def rebalance(current: Allocation, inProgress: Set[Shard]) = {
    current.values.flatten.toList
  }
}