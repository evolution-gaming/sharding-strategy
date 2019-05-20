package com.evolutiongaming.cluster.sharding

import akka.actor.Address

object SingleNodeStrategy {

  def apply(address: => Option[Address], toAddress: Region => Address): ShardingStrategy = {

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
          if toAddress(region) != address
          if regionByAddress(address, current).isDefined
          shard <- shards
        } yield shard

        shards.toList.sorted
      }

      private def regionByAddress(address: Address, current: Allocation) = {
        current.keys find { region => toAddress(region) == address }
      }
    }
  }
}