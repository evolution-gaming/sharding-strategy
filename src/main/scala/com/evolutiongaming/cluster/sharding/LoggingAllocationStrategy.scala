package com.evolutiongaming.cluster.sharding

import akka.actor.Address
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext

import scala.concurrent.Future

class LoggingAllocationStrategy(
  log: (() => String) => Unit,
  strategy: ShardAllocationStrategy,
  toGlobal: Address => Address) extends ShardAllocationStrategy {

  private implicit val ec = CurrentThreadExecutionContext

  def allocateShard(requester: Region, shard: Shard, current: Allocation): Future[Region] = {
    val region = strategy.allocateShard(requester, shard, current)
    region foreach { region =>
      def msg = s"allocate $shard to ${ regionToStr(region) }, " +
        s"requester: ${ regionToStr(requester) }, " +
        s"current: ${ allocationToStr(current) }"

      log(() => msg)
    }
    region
  }

  def rebalance(current: Allocation, inProgress: Set[Shard]): Future[Set[Shard]] = {
    val shards = strategy.rebalance(current, inProgress)
    shards foreach { shards =>
      if (shards.nonEmpty) {
        def msg = s"rebalance ${ shards mkString "," }, " +
          s"${ if (inProgress.nonEmpty) s"inProgress(${ inProgress.size }): ${ iterToStr(inProgress) }" else "" }" +
          s"current: ${ allocationToStr(current) }"

        log(() => msg)
      }
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