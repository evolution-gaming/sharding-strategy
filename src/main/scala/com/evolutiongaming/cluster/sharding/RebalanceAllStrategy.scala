package com.evolutiongaming.cluster.sharding

object RebalanceAllStrategy extends ShardingStrategy {

  def allocate(requester: Region, shard: Shard, current: Allocation) = None

  def rebalance(current: Allocation, inProgress: Set[Shard]) = {
    current.values.flatten.toList
  }
}