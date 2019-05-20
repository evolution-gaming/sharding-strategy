package com.evolutiongaming.cluster.sharding

object RebalanceAllStrategy {

  def apply(): ShardingStrategy = new ShardingStrategy {

    def allocate(requester: Region, shard: Shard, current: Allocation) = None

    def rebalance(current: Allocation, inProgress: Set[Shard]) = {
      current.values.flatten.toList
    }
  }
}