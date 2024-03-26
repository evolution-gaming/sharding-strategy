package com.evolutiongaming.cluster.sharding

import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import cats.effect.{Clock, Ref, Sync}
import cats.implicits._
import cats.{Applicative, FlatMap, Monad, ~>}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.ToFuture

import scala.concurrent.duration.FiniteDuration

/**
  * Interface of the pluggable shard allocation and rebalancing logic used by the [[akka.cluster.sharding.ShardCoordinator]].
  *
  * @see check [[ShardAllocationStrategy]] from [[https://github.com/akka/akka/blob/main/akka-cluster-sharding/src/main/scala/akka/cluster/sharding/ShardCoordinator.scala]]
  */
trait ShardingStrategy[F[_]] {

  /**
    * Invoked when the location of a new shard is to be decided.
    *
    * @param requester actor reference to the [[akka.cluster.sharding.ShardRegion]] that requested the location of the
    *                  shard, can be returned if preference should be given to the node where the shard was first accessed
    * @param shard     the id of the shard to allocate
    * @param current   all actor refs to `ShardRegion` and their current allocated shards, in the order they were allocated
    * @return an effect `F` of the actor ref of the [[ShardRegion]] that is to be responsible for the shard, must be one of
    *         the references included in the `current` parameter
    */
  def allocate(requester: Region, shard: Shard, current: Allocation): F[Option[Region]]

  /**
    * Invoked periodically to decide which shards to rebalance to another location.
    *
    * @param current    all actor refs to `ShardRegion` and their current allocated shards, in the order they were allocated
    * @param inProgress set of shards that are currently being rebalanced, i.e. you should not include these in the returned set
    * @return an effect `F` of the shards to be migrated, may be empty to skip rebalance in this round
    */
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

        def rebalance(current: Allocation, inProgress: Set[Shard]) = {
          val allocation = if (inProgress.isEmpty) current else current.map { case (k, v) => (k, v filterNot inProgress) }
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

    def of[F[_] : Sync](
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

  /**
    * Adds a track of unallocated shards.
    * If we configured sharding in such a way that it automatically re-create shards and entities inside of it after a rebalance,
    * we know that all shards returned from rebalance call would be allocated in the nearest future.
    * Term unallocated shard in this context means such shard that was stopped on a previous node, but not yet allocated on a new one.
    */
  object TrackUnallocated {

    def of[F[_] : Sync](
      strategy: ShardingStrategy[F],
    ): F[ShardingStrategy[F]] =
      for {
        unallocatedShards <- Ref[F].of(Set.empty[Shard])
      } yield {
        apply(unallocatedShards, strategy)
      }

    def apply[F[_] : FlatMap](
      unallocatedShards: Ref[F, Set[Shard]],
      strategy: ShardingStrategy[F],
    ): ShardingStrategy[F] = {

      new ShardingStrategy[F] {

        def allocate(requester: Region, shard: Shard, current: Allocation) =
          for {
            region <- strategy.allocate(requester, shard, current)
            _ <- unallocatedShards.update(_.filterNot(_ == shard))
          } yield region

        def rebalance(current: Allocation, inProgress: Set[Shard]) =
          for {
            lastUnallocated <- unallocatedShards.get
            shards <- strategy.rebalance(current, inProgress ++ lastUnallocated)
            _ <- unallocatedShards.update(_ ++ shards)
          } yield shards
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


    def shardRebalanceCooldown(cooldown: FiniteDuration)(implicit F: Sync[F]): F[ShardingStrategy[F]] = {
      ShardRebalanceCooldown.of[F](cooldown, self)
    }

    def withTrackUnallocated(implicit F: Sync[F]): F[ShardingStrategy[F]] =
      TrackUnallocated.of(self)

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
