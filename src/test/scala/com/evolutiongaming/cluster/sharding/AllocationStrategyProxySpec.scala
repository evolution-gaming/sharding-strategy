package com.evolutiongaming.cluster.sharding

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.evolutiongaming.catshelper.FromFuture
import com.evolutiongaming.cluster.sharding.AllocationStrategyHelper.*
import com.evolutiongaming.cluster.sharding.IOSuite.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.IndexedSeq
import scala.concurrent.duration.*

class AllocationStrategyProxySpec extends AsyncFunSuite with ActorSpec with Matchers {

  private val region = RegionOf(actorSystem)
  private val shard = "shard"
  private val ignore = (msg: () => String) => { msg(); () }

  private implicit val addressOf: AddressOf = AddressOf(actorSystem)

  test("allocate") {
    val allocation = Map((region, IndexedSeq(shard)))
    val result = for {
      strategy0 <- ShardingStrategy.empty[IO].shardRebalanceCooldown(1.second)
      strategy = strategy0
        .toAllocationStrategy()
        .logging(ignore)
        .toShardingStrategy[IO]
        .toAllocationStrategy()
      region1 <- FromFuture[IO].apply { strategy.allocateShard(region, shard, allocation) }
    } yield {
      region1 shouldEqual region
    }
    result.run()
  }

  test("rebalance") {
    val allocation = Map((region, IndexedSeq(shard)))
    val result = for {
      strategy0 <- ShardingStrategy.empty[IO].shardRebalanceCooldown(1.second)
      strategy = strategy0
        .toAllocationStrategy()
        .logging(ignore)
        .toShardingStrategy[IO]
        .toAllocationStrategy()
      shards <- FromFuture[IO].apply { strategy.rebalance(allocation, Set(shard)) }
    } yield {
      shards shouldEqual Set.empty
    }

    result.run()
  }
}
