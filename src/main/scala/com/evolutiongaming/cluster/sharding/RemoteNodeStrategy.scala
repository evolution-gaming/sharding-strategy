package com.evolutiongaming.cluster.sharding

import akka.actor.ActorRef
import akka.cluster.sharding.ShardRegion

object RemoteNodeStrategy extends ShardingStrategy {

  private def isLocal(region: ActorRef) = region.path.address.hasLocalScope

  def allocate(requesterRef: ActorRef, requester: Region, shard: Shard, current: Allocation) = {
    val regions = current.keys
    regions find { region => !isLocal(region) }
  }

  def rebalance(current: Allocation, inProgress: Set[Shard]) = {
    val (local, remote) = current.partition { case (region, _) => isLocal(region) }
    if (remote.nonEmpty) local.values.flatten.toList else List.empty[ShardRegion.ShardId]
  }
}