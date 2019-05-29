package com.evolutiongaming.cluster.sharding

import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import cats.FlatMap
import com.evolutiongaming.catshelper.FromFuture

import scala.concurrent.ExecutionContext

object AllocationStrategyHelper {

  implicit class AllocationStrategyOps(val self: ShardAllocationStrategy) extends AnyVal {

    def toShardingStrategy[F[_] : FlatMap : FromFuture]: ShardingStrategy[F] = {
      ShardingStrategyProxy(self)
    }

    def logging(
      log: (() => String) => Unit)(implicit
      addressOf: AddressOf,
      executor: ExecutionContext
    ): ShardAllocationStrategy = {
      LoggingAllocationStrategy(log, self, addressOf)
    }
  }
}
