package com.evolutiongaming.cluster.sharding

import cats.effect.IO
import com.evolutiongaming.catshelper.FromFuture
import org.scalatest.{AsyncFunSuite, Matchers}
import com.evolutiongaming.cluster.sharding.IOSuite._
import com.evolutiongaming.cluster.sharding.AllocationStrategyHelper._

import scala.concurrent.duration._
import scala.collection.immutable.IndexedSeq

class AllocationStrategyProxySpec extends AsyncFunSuite with ActorSpec with Matchers {
  private val region = RegionOf(actorSystem)
  private val shard = "shard"

  private implicit val addressOf = AddressOf(actorSystem)

  test("allocate") {
    val allocation = Map((region, IndexedSeq(shard)))
    val result = for {
      strategy0 <- ShardingStrategy.empty[IO].shardRebalanceCooldown(1.second)
      strategy   = strategy0
        .toAllocationStrategy()
        .logging(_ => ())
        .toShardingStrategy[IO]
        .toAllocationStrategy()
      region1   <- FromFuture[IO].apply { strategy.allocateShard(region, shard, allocation) }
    } yield {
      region1 shouldEqual region
    }
    result.run()
  }

  test("rebalance") {
    val allocation = Map((region, IndexedSeq(shard)))
    val result = for {
      strategy0 <- ShardingStrategy.empty[IO].shardRebalanceCooldown(1.second)
      strategy   = strategy0
        .toAllocationStrategy()
        .logging(_ => ())
        .toShardingStrategy[IO]
        .toAllocationStrategy()
      shards    <- FromFuture[IO].apply { strategy.rebalance(allocation, Set(shard)) }
    } yield {
      shards shouldEqual Set.empty
    }

    result.run()
  }
}
