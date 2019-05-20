package com.evolutiongaming.cluster.sharding

import akka.actor.Address
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

object AllocationStrategyHelper {

  implicit class AllocationStrategyOps(val self: ShardAllocationStrategy) extends AnyVal {

    def toShardingStrategy(timeout: Duration): ShardingStrategy = ShardingStrategyProxy(timeout, self)

    def logging(
      toGlobal: Address => Address)(
      log: (() => String) => Unit)(implicit
      executor: ExecutionContext
    ): ShardAllocationStrategy = {
      LoggingAllocationStrategy(log, self, toGlobal)
    }
  }
}
