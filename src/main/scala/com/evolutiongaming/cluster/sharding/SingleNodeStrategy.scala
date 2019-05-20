package com.evolutiongaming.cluster.sharding

import akka.actor.Address

object SingleNodeStrategy {

  def apply(address: => Option[Address], addressOf: AddressOf): ShardingStrategy = {

    def regionByAddress(address: Address, current: Allocation) = {
      current.keys find { region => addressOf(region) == address }
    }

    new ShardingStrategy {

      def allocate(requester: Region, shard: Shard, current: Allocation) = {
        for {
          address <- address
          region <- regionByAddress(address, current)
        } yield region
      }

      def rebalance(current: Allocation, inProgress: Set[Shard]) = {
        val shards = for {
          address <- address.toIterable
          (region, shards) <- current
          if shards.nonEmpty
          if addressOf(region) != address
          if regionByAddress(address, current).isDefined
          shard <- shards
        } yield shard

        shards.toList.sorted
      }
    }
  }
}