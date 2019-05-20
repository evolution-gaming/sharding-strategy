package com.evolutiongaming.cluster.sharding

import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object ShardingStrategyProxy {

  def apply(timeout: Duration, strategy: ShardAllocationStrategy): ShardingStrategy = {

    def await[T](future: Future[T]): T = Await.result(future, timeout)

    new ShardingStrategy {
      def allocate(requester: Region, shard: Shard, current: Allocation) = {
        val region = await {
          strategy.allocateShard(requester, shard, current)
        }
        Some(region)
      }

      def rebalance(current: Allocation, inProgress: Set[Shard]) = {
        val shards = await {
          strategy.rebalance(current, inProgress)
        }
        shards.toList
      }
    }
  }
}
