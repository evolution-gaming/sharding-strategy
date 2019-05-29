package com.evolutiongaming.cluster.sharding

import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import cats.effect.concurrent.Ref
import cats.effect.{Clock, Sync}
import cats.implicits._
import cats.{Applicative, FlatMap, Monad, ~>}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.ToFuture

import scala.collection.immutable.IndexedSeq
import scala.concurrent.duration.FiniteDuration

trait ShardingStrategy[F[_]] {

  def allocate(requester: Region, shard: Shard, current: Allocation): F[Option[Region]]

  def rebalance(current: Allocation, inProgress: Set[Shard]): F[List[Shard]]
}

object ShardingStrategy {

  def empty[F[_] : Applicative]: ShardingStrategy[F] = new ShardingStrategy[F] {

    def allocate(requester: Region, shard: Shard, current: Allocation) = none[Region].pure[F]

    def rebalance(current: Allocation, inProgress: Set[Shard]) = List.empty[Shard].pure[F]
  }


  def requesterAllocation[F[_] : Applicative]: ShardingStrategy[F] = new ShardingStrategy[F] {

    def allocate(requester: Region, shard: Shard, current: Allocation) = requester.some.pure[F]

    def rebalance(current: Allocation, inProgress: Set[Shard]) = List.empty[Shard].pure[F]
  }


  object TakeShards {

    def apply[F[_] : Monad](numberOfShards: => F[Int], strategy: ShardingStrategy[F]): ShardingStrategy[F] = {
      new ShardingStrategy[F] {

        def allocate(requester: Region, shard: Shard, current: Allocation) = {
          strategy.allocate(requester, shard, current)
        }

        def rebalance(current: Allocation, inProgress: Set[Shard]) = {

          def rebalance(numberOfShards: Int) = {
            val size = inProgress.size
            if (size < numberOfShards) {
              strategy.rebalance(current, inProgress).map(_.take(numberOfShards - size))
            } else {
              List.empty[Shard].pure[F]
            }
          }

          for {
            numberOfShards <- numberOfShards
            shards         <- rebalance(numberOfShards)
          } yield shards
        }
      }
    }
  }


  object FilterRegions {

    def apply[F[_] : Monad](filter: F[Region => Boolean], strategy: ShardingStrategy[F]): ShardingStrategy[F] = {
      new ShardingStrategy[F] {

        def allocate(requester: Region, shard: Shard, current: Allocation) = {

          def allocate(included: Allocation) = {
            for {
              region <- strategy.allocate(requester, shard, included)
            } yield {
              if (included contains requester) region
              else region filter (_ != requester) orElse included.keys.headOption
            }
          }

          for {
            filter   <- filter
            included  = current.filter { case (region, _) => filter(region) }
            region   <- if (included.nonEmpty) allocate(included) else none[Region].pure[F]
          } yield region
        }

        def rebalance(current: Allocation, inProgress: Set[Shard]) = {

          def rebalance(included: Allocation, excluded: Allocation) = {
            val excludedShards = excluded.values.flatten.toList
            for {
              shards <- strategy.rebalance(included, inProgress)
            } yield {
              excludedShards ++ shards
            }
          }

          for {
            filter               <- filter
            (included, excluded)  = current.partition { case (region, _) => filter(region) }
            shards               <- if (included.nonEmpty) rebalance(included, excluded) else List.empty[Shard].pure[F]
          } yield shards
        }
      }
    }
  }


  object FilterShards {

    def apply[F[_] : FlatMap](filter: F[Shard => Boolean], strategy: ShardingStrategy[F]): ShardingStrategy[F] = {
      new ShardingStrategy[F] {

        def allocate(requester: Region, shard: Shard, current: Allocation) = {
          strategy.allocate(requester, shard, current)
        }

        def rebalance(current: Allocation, inProgress: Set[Shard]) = {
          for {
            shards <- strategy.rebalance(current, inProgress)
            filter <- filter
          } yield {
            shards filter filter
          }
        }
      }
    }
  }


  object Threshold {

    def apply[F[_] : Monad](threshold: => F[Int], strategy: ShardingStrategy[F]): ShardingStrategy[F] = {

      new ShardingStrategy[F] {

        def allocate(requester: Region, shard: Shard, current: Allocation) = {
          strategy.allocate(requester, shard, current)
        }

        def rebalance(current: Allocation, inProgress: Set[Shard]) = {
          for {
            shards    <- strategy.rebalance(current, inProgress)
            threshold <- threshold
          } yield {
            if ((shards lengthCompare threshold) >= 0) shards
            else List.empty[Shard]
          }
        }
      }
    }
  }


  object AllocationStrategyProxy {

    def apply[F[_] : FlatMap : ToFuture](
      strategy: ShardingStrategy[F],
      fallback: Allocate = Allocate.Default
    ): ShardAllocationStrategy = {

      new ShardAllocationStrategy {

        def allocateShard(requester: Region, shardId: Shard, current: Allocation) = {
          val region = for {
            region <- strategy.allocate(requester, shardId, current)
          } yield {
            region getOrElse fallback(requester, shardId, current)
          }
          ToFuture[F].apply { region }
        }

        def rebalance(current: Map[Region, IndexedSeq[Shard]], inProgress: Set[Shard]) = {
          val allocation = if (inProgress.isEmpty) current else current.mapValues { _ filterNot inProgress }
          val shards = for {
            shards <- strategy.rebalance(allocation, inProgress)
          } yield {
            shards.toSet
          }
          ToFuture[F].apply { shards }
        }
      }
    }
  }


  object Logging {

    def apply[F[_] : Monad](
      strategy: ShardingStrategy[F],
      log: (() => String) => F[Unit],
      addressOf: AddressOf,
    ): ShardingStrategy[F] = {

      def allocationToStr(current: Allocation) = {
        current map { case (region, shards) =>
          val regionStr = regionToStr(region)
          val shardsStr = iterToStr(shards)
          val size = shards.size
          s"$regionStr($size): $shardsStr"
        } mkString ", "
      }

      def iterToStr(iter: Iterable[_]) = {
        iter.toSeq.map { _.toString }.sorted.mkString("[", ",", "]")
      }

      def regionToStr(region: Region) = {
        val address = addressOf(region)
        val host = address.host.fold("none") { _.toString }
        val port = address.port.fold("none") { _.toString }
        s"$host:$port"
      }

      new ShardingStrategy[F] {

        def allocate(requester: Region, shard: Shard, current: Allocation) = {
          for {
            region <- strategy.allocate(requester, shard, current)
            msg     = () => s"allocate $shard to ${ region.fold("none") { regionToStr } }, " +
              s"requester: ${ regionToStr(requester) }, " +
              s"current: ${ allocationToStr(current) }"
            _ <- log(msg)
          } yield {
            region
          }
        }

        def rebalance(current: Allocation, inProgress: Set[Shard]) = {
          for {
            shards <- strategy.rebalance(current, inProgress)
            _      <- if (shards.nonEmpty) {
              val msg = () => s"rebalance ${ shards mkString "," }, " +
                s"${ if (inProgress.nonEmpty) s"inProgress(${ inProgress.size }): ${ iterToStr(inProgress) }, " else "" }" +
                s"current: ${ allocationToStr(current) }"

              log(msg)
            } else {
              ().pure[F]
            }
          } yield {
            shards
          }
        }
      }
    }
  }


  /**
    * Adds shard rebalance cooldown in order to avoid unnecessary flapping
    */
  object ShardRebalanceCooldown {

    def of[F[_] : Sync : Clock](
      cooldown: FiniteDuration,
      strategy: ShardingStrategy[F],
    ): F[ShardingStrategy[F]] = {
      for {
        allocationTime <- Ref[F].of(Map.empty[Shard, Long])
      } yield {
        apply(cooldown, strategy, allocationTime)
      }
    }

    def apply[F[_] : FlatMap : Clock](
      cooldown: FiniteDuration,
      strategy: ShardingStrategy[F],
      allocationTime: Ref[F, Map[Shard, Long]]
    ): ShardingStrategy[F] = {

      new ShardingStrategy[F] {

        def allocate(requester: Region, shard: Shard, current: Allocation) = {
          for {
            region    <- strategy.allocate(requester, shard, current)
            timestamp <- Clock[F].millis
            _         <- allocationTime.update { _.updated(shard, timestamp) }
          } yield {
            region
          }
        }

        def rebalance(current: Allocation, inProgress: Set[Shard]) = {
          for {
            shards         <- strategy.rebalance(current, inProgress)
            now            <- Clock[F].millis
            allocationTime <- allocationTime.get
          } yield for {
            shard <- shards
            timestamp = allocationTime get shard
            if timestamp.forall { _ + cooldown.toMillis <= now }
          } yield {
            shard
          }
        }
      }
    }
  }


  implicit class ShardingStrategyOps[F[_]](val self: ShardingStrategy[F]) extends AnyVal {

    /**
      * At most n shards will be rebalanced at the same time
      */
    def takeShards(numberOfShards: => F[Int])(implicit F: Monad[F]): ShardingStrategy[F] = {
      TakeShards(numberOfShards, self)
    }


    /**
      * Prevents rebalance until threshold of number of shards reached
      */
    def rebalanceThreshold(numberOfShards: => F[Int])(implicit F: Monad[F]): ShardingStrategy[F] = {
      Threshold(numberOfShards, self)
    }


    /**
      * Allows shards allocation on included regions and rebalances off from excluded
      */
    def filterRegions(filter: F[Region => Boolean])(implicit F: Monad[F]): ShardingStrategy[F] = {
      FilterRegions(filter, self)
    }


    def filterShards(filter: F[Shard => Boolean])(implicit F: FlatMap[F]): ShardingStrategy[F] = {
      FilterShards(filter, self)
    }


    def shardRebalanceCooldown(
      cooldown: FiniteDuration)(implicit
      F: Sync[F],
      clock: Clock[F]
    ): F[ShardingStrategy[F]] = {
      ShardRebalanceCooldown.of[F](cooldown, self)
    }


    def toAllocationStrategy(
      fallback: Allocate = Allocate.Default)(implicit
      F: FlatMap[F],
      toFuture: ToFuture[F]
    ): ShardAllocationStrategy = {
      AllocationStrategyProxy(self, fallback)
    }


    def logging(
      log: (() => String) => F[Unit])(implicit
      F: Monad[F],
      addressOf: AddressOf
    ): ShardingStrategy[F] = {
      Logging[F](self, log, addressOf)
    }


    def mapK[G[_]](f: F ~> G): ShardingStrategy[G] = new ShardingStrategy[G] {

      def allocate(requester: Region, shard: Shard, current: Allocation) = {
        f(self.allocate(requester, shard, current))
      }

      def rebalance(current: Allocation, inProgress: Set[Shard]) = {
        f(self.rebalance(current, inProgress))
      }
    }
  }
}
