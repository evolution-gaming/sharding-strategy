package com.evolutiongaming.cluster.sharding

import akka.actor.Address
import cats.Applicative
import cats.implicits.*

object SingleNodeStrategy {

  def apply[F[_] : Applicative](address: => Option[Address], addressOf: AddressOf): ShardingStrategy[F] = {

    def regionByAddress(address: Address, current: Allocation) = {
      current.keys find { region => addressOf(region) == address }
    }

    new ShardingStrategy[F] {

      def allocate(requester: Region, shard: Shard, current: Allocation) = {
        val region = for {
          address <- address
          region  <- regionByAddress(address, current)
        } yield region
        region.pure[F]
      }

      def rebalance(current: Allocation, inProgress: Set[Shard]) = {
        val shards = for {
          address          <- address.toIterable
          (region, shards) <- current
          if shards.nonEmpty
          if addressOf(region) != address
          if regionByAddress(address, current).isDefined
          shard            <- shards
        } yield shard

        shards.toList.sorted.pure[F]
      }
    }
  }
}