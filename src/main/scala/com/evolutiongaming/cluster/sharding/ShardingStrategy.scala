package com.evolutiongaming.cluster.sharding

import akka.actor.Address
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.compat.Platform
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait ShardingStrategy {

  def allocate(requester: Region, shard: Shard, current: Allocation): Option[Region]

  def rebalance(current: Allocation, inProgress: Set[Shard]): List[Shard]
}

object ShardingStrategy {

  class TakeShards(n: => Int, strategy: ShardingStrategy) extends ShardingStrategy {

    def allocate(requester: Region, shard: Shard, current: Allocation) = {
      strategy.allocate(requester, shard, current)
    }

    def rebalance(current: Allocation, inProgress: Set[Shard]) = {
      val size = inProgress.size
      if (size < n) strategy.rebalance(current, inProgress) take (n - size)
      else Nil
    }
  }


  class FilterRegions(f: Region => Boolean, strategy: ShardingStrategy) extends ShardingStrategy {

    def allocate(requester: Region, shard: Shard, current: Allocation) = {
      val included = current.filter { case (region, _) => f(region) }

      if (included.isEmpty) None
      else {
        val region = strategy.allocate(requester, shard, included)
        if (included contains requester) region
        else region filter (_ != requester) orElse included.keys.headOption
      }
    }

    def rebalance(current: Allocation, inProgress: Set[Shard]) = {
      val (included, excluded) = current.partition { case (region, _) => f(region) }
      if (included.isEmpty) Nil
      else {
        val excludedShards = excluded.values.flatten.toList
        val shards = strategy.rebalance(included, inProgress)
        excludedShards ++ shards
      }
    }
  }


  class FilterShards(f: Shard => Boolean, strategy: ShardingStrategy) extends ShardingStrategy {

    def allocate(requester: Region, shard: Shard, current: Allocation) = {
      strategy.allocate(requester, shard, current)
    }

    def rebalance(current: Allocation, inProgress: Set[Shard]) = {
      val shards = strategy.rebalance(current, inProgress)
      shards filter f
    }
  }


  /**
    * Per-shard allocation and rebalance based on matching shard's "role" with node roles
    */
  class FilterByRole(
    shardRole: Shard => Option[String],
    toAddress: Region => Address,
    clusterMembersWithRoles: => Map[Address, Set[String]],
    strategy: ShardingStrategy) extends ShardingStrategy {

    def allocate(requester: Region, shard: Shard, current: Allocation): Option[Region] = {

      shardRole(shard) match {
        case Some(role) =>

          val included = for {
            (memberAddress, roles) <- clusterMembersWithRoles if roles contains role
            (region, shards) <- current if toAddress(region) == memberAddress
          } yield (region, shards)

          if (included.isEmpty) None
          else {
            val region = strategy.allocate(requester, shard, included)
            region flatMap { region =>
              if (included contains region) Some(region)
              else if (included contains requester) Some(requester)
              else included.keys.headOption
            }
          }

        case None       => strategy.allocate(requester, shard, current)
      }
    }

    def rebalance(current: Allocation, inProgress: Set[Shard]): List[Shard] = {

      val excludedShards = (for {
        (region, shards) <- current
        regionRoles = clusterMembersWithRoles.getOrElse(toAddress(region), Set.empty)
        shard <- shards
        shardRole <- shardRole(shard) if !(regionRoles contains shardRole)
      } yield shard).toSet

      val included = current map {
        case (region, shards) => region -> shards.filter(shard => !(excludedShards contains shard))
      }

      val shards = strategy.rebalance(included, inProgress)
      (excludedShards ++ shards).toList
    }
  }


  class Threshold(n: => Int, strategy: ShardingStrategy) extends ShardingStrategy {

    def allocate(requester: Region, shard: Shard, current: Allocation) = {
      strategy.allocate(requester, shard, current)
    }

    def rebalance(current: Allocation, inProgress: Set[Shard]) = {
      val shards = strategy.rebalance(current, inProgress)
      if ((shards lengthCompare n) >= 0) shards else Nil
    }
  }


  object Empty extends ShardingStrategy {

    def allocate(requester: Region, shard: Shard, current: Allocation) = None

    def rebalance(current: Allocation, inProgress: Set[Shard]) = Nil
  }


  object RequesterAllocation extends ShardingStrategy {

    def allocate(requester: Region, shard: Shard, current: Allocation) = Some(requester)

    def rebalance(current: Allocation, inProgress: Set[Shard]) = Nil
  }


  class AllocationStrategyProxy(strategy: ShardingStrategy, fallback: Allocate = Allocate.Default)
    extends ShardAllocationStrategy {

    def allocateShard(requester: Region, shardId: Shard, current: Allocation) = {
      if (current.size <= 1) Future.successful(requester)
      else {
        val region = strategy.allocate(requester, shardId, current) getOrElse {
          fallback(requester, shardId, current)
        }
        Future.successful(region)
      }
    }

    def rebalance(current: Map[Region, IndexedSeq[Shard]], inProgress: Set[Shard]) = {
      val allocation = if (inProgress.isEmpty) current else current.mapValues { _ filterNot inProgress }
      val shards = strategy.rebalance(allocation, inProgress)
      Future.successful(shards.toSet)
    }
  }


  class Logging(
    log: (() => String) => Unit,
    strategy: ShardingStrategy,
    toGlobal: Address => Address) extends ShardingStrategy {

    def allocate(requester: Region, shard: Shard, current: Allocation) = {
      val region = strategy.allocate(requester, shard, current)

      def msg = s"allocate $shard to ${ region.fold("none") { regionToStr } }, " +
        s"requester: ${ regionToStr(requester) }, " +
        s"current: ${ allocationToStr(current) }"

      log(() => msg)
      region
    }

    def rebalance(current: Allocation, inProgress: Set[Shard]) = {
      val shards = strategy.rebalance(current, inProgress)
      if (shards.nonEmpty) {
        def msg = s"rebalance ${ shards mkString "," }, " +
          s"${ if (inProgress.nonEmpty) s"inProgress(${ inProgress.size }): ${ iterToStr(inProgress) }" else "" }" +
          s"current: ${ allocationToStr(current) }"

        log(() => msg)
      }
      shards
    }

    private def allocationToStr(current: Allocation) = {
      current map { case (region, shards) =>
        val regionStr = regionToStr(region)
        val shardsStr = iterToStr(shards)
        val size = shards.size
        s"$regionStr($size): $shardsStr"
      } mkString ", "
    }

    private def iterToStr(iter: Iterable[_]) = {
      iter.toSeq.map { _.toString }.sorted.mkString("[", ",", "]")
    }

    private def regionToStr(region: Region) = {
      val address = toGlobal(region.path.address)
      val host = address.host.fold("none") { _.toString }
      val port = address.port.fold("none") { _.toString }
      s"$host:$port"
    }
  }


  /**
    * Adds shard rebalance cooldown in order to avoid unnecessary flapping
    */
  class ShardRebalanceCooldown(cooldown: FiniteDuration, strategy: ShardingStrategy) extends ShardingStrategy {

    private val allocationTime = mutable.Map.empty[Shard, Long]

    def allocate(requester: Region, shard: Shard, current: Allocation) = {
      val region = strategy.allocate(requester, shard, current)
      allocationTime.put(shard, Platform.currentTime)
      region
    }

    def rebalance(current: Allocation, inProgress: Set[Shard]) = {
      val shards = strategy.rebalance(current, inProgress)
      val now = Platform.currentTime
      shards filter { shard =>
        val time = allocationTime get shard
        time forall { _ + cooldown.toMillis <= now }
      }
    }
  }


  implicit class ShardingStrategyOps(val self: ShardingStrategy) extends AnyVal {

    /**
      * At most n shards will be rebalanced at the same time
      */
    def takeShards(n: => Int): ShardingStrategy = new TakeShards(n, self)

    /**
      * Prevents rebalance until threshold of number of shards reached
      */
    def rebalanceThreshold(n: => Int): ShardingStrategy = new Threshold(n, self)

    /**
      * Allows shards allocation on included regions and rebalances off from excluded
      */
    def filterRegions(f: Region => Boolean): ShardingStrategy = new FilterRegions(f, self)

    def filterShards(f: Shard => Boolean): ShardingStrategy = new FilterShards(f, self)

    /**
      * Per-shard allocation and rebalance based on matching shard's "role" with node roles
      */
    def filterByRole(
      shardRole: Shard => Option[String],
      toAddress: Region => Address,
      clusterMembersWithRoles: => Map[Address, Set[String]]): ShardingStrategy =
      new FilterByRole(shardRole, toAddress, clusterMembersWithRoles, self)

    def shardRebalanceCooldown(cooldown: FiniteDuration): ShardingStrategy = new ShardRebalanceCooldown(cooldown, self)

    def toAllocationStrategy(fallback: Allocate = Allocate.Default): ShardAllocationStrategy = {
      new AllocationStrategyProxy(self, fallback)
    }

    def logging(toGlobal: Address => Address)(log: (() => String) => Unit): ShardingStrategy = {
      new Logging(log, self, toGlobal)
    }
  }
}
