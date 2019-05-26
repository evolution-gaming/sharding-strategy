package com.evolutiongaming.cluster.sharding

import akka.actor.Address
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import cats.FlatMap
import com.evolutiongaming.catshelper.FromFuture

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

object AllocationStrategyHelper {

  implicit class AllocationStrategyOps(val self: ShardAllocationStrategy) extends AnyVal {

    def toShardingStrategy[F[_] : FlatMap : FromFuture](timeout: Duration): ShardingStrategy[F] = {
      ShardingStrategyProxy(self)
    }

    def logging(
      toGlobal: Address => Address)(
      log: (() => String) => Unit)(implicit
      executor: ExecutionContext
    ): ShardAllocationStrategy = {
      LoggingAllocationStrategy(log, self, toGlobal)
    }
  }
}
