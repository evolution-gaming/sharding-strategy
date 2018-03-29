package com.evolutiongaming.cluster.sharding

import akka.actor.ActorRef

object LeastShardsStrategy extends ShardingStrategy {

  private val reversedOrdering = Ordering[Int].reverse

  def allocate(requesterRef: ActorRef, requester: Region, shard: Shard, current: Allocation) = {

    val zero = (Int.MaxValue, List.empty[Region])
    val (min, regions) = current.foldLeft(zero) { case ((min, regions), (region, shards)) =>
      val shardsSize = shards.size
      if (shardsSize < min) (shardsSize, region :: Nil)
      else if (shardsSize == min) (min, region :: regions)
      else (min, regions)
    }
    if (regions.size == current.size) None
    else if (current forall { case (_, shards) => shards.size == min }) None
    else if (regions contains requester) Some(requester)
    else regions.headOption
  }

  def rebalance(current: Allocation, inProgress: Set[Shard]) = {

    val regionsSize = current.size
    if (regionsSize >= 2) {
      val shards = current.map { case (_, shards) => shards.toList }.toSeq.sortBy(_.size)
      val shardsSize = shards.foldLeft(0) { case (sum, shards) => sum + shards.size }
      val distribution = {
        val shardsPerRegion = shardsSize / regionsSize
        val distribution = List.fill(regionsSize)(shardsPerRegion)
        val remainder = shardsSize % regionsSize
        if (remainder == 0) distribution
        else {
          val zero = (remainder, List.empty[Int])
          val (_, result) = distribution.foldLeft(zero) { case ((remainder, xs), x) =>
            if (remainder > 0) (remainder - 1, (x + 1) :: xs)
            else (remainder, x :: xs)
          }
          result
        }
      }

      val result = (shards zip distribution)
        .collect { case (shards, size) if shards.size > size => (shards drop size).zipWithIndex }
        .flatten
        .sortBy { case (_, idx) => idx }(reversedOrdering)
        .map { case (shard, _) => shard }
        .toList

      result
    } else {
      Nil
    }
  }
}
