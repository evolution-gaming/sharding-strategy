package com.evolutiongaming.cluster.sharding

import akka.actor.{ActorRef, Address}
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object AllocationStrategyHelper {

  class ShardingStrategyProxy(timeout: Duration, strategy: ShardAllocationStrategy) extends ShardingStrategy {

    def allocate(requesterRef: ActorRef, requester: Region, shard: Shard, current: Allocation) = {
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

    private def await[T](future: Future[T]): T = Await.result(future, timeout)
  }

  implicit class AllocationStrategyOps(val self: ShardAllocationStrategy) extends AnyVal {

    def toShardingStrategy(timeout: Duration): ShardingStrategy = new ShardingStrategyProxy(timeout, self)

    def logging(toGlobal: Address => Address)(log: (() => String) => Unit): ShardAllocationStrategy = {
      new LoggingAllocationStrategy(log, self, toGlobal)
    }
  }
}
