package com.evolutiongaming.cluster.sharding

import akka.actor.ActorRef
import akka.cluster.sharding.ShardRegion

object RemoteNodeStrategy {

  def apply(): ShardingStrategy = {
    apply(_.path.address.hasLocalScope)
  }

  def apply(isLocal: ActorRef => Boolean): ShardingStrategy = {
    new ShardingStrategy {

      def allocate(requester: ActorRef, shard: Shard, current: Allocation) = {
        val regions = current.keys
        regions find { region => !isLocal(region) }
      }

      def rebalance(current: Allocation, inProgress: Set[Shard]) = {
        val (local, remote) = current.partition { case (region, _) => isLocal(region) }
        if (remote.nonEmpty) local.values.flatten.toList else List.empty[ShardRegion.ShardId]
      }
    }
  }
}