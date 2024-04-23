package com.evolutiongaming.cluster.sharding

import akka.actor.ActorRef
import cats.Applicative
import cats.implicits.*

object RemoteNodeStrategy {

  def apply[F[_] : Applicative]: ShardingStrategy[F] = {
    apply(_.path.address.hasLocalScope)
  }

  def apply[F[_] : Applicative](isLocal: ActorRef => Boolean): ShardingStrategy[F] = {
    new ShardingStrategy[F] {

      def allocate(requester: ActorRef, shard: Shard, current: Allocation) = {
        val regions = current.keys
        val region = regions.find { region => !isLocal(region) }
        region.pure[F]
      }

      def rebalance(current: Allocation, inProgress: Set[Shard]) = {
        val (local, remote) = current.partition { case (region, _) => isLocal(region) }
        val shards = if (remote.nonEmpty) local.values.flatten.toList else List.empty[Shard]
        shards.pure[F]
      }
    }
  }
}