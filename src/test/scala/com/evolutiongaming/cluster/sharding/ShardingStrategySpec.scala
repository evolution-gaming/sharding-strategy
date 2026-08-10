package com.evolutiongaming.cluster.sharding

import cats.Id
import cats.effect.SyncIO
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.IndexedSeq

class ShardingStrategySpec extends AnyWordSpec with ActorSpec with Matchers {

  "ShardingStrategy" should {

    "filterRegions" in {

      val strategy = ShardingStrategy.requesterAllocation[Id].filterRegions(_ == region1)

      val allocation = Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq(shard2, shard3),
        region3 -> IndexedSeq(shard4),
      )

      strategy.rebalance(allocation, Set.empty) shouldEqual List(shard2, shard3, shard4)
      strategy.rebalance(Map.empty, Set.empty) shouldEqual Nil

      strategy.allocate(region2, shard2, Map(region2 -> IndexedSeq(shard1))) shouldEqual None
      strategy.allocate(region1, shard2, Map(region1 -> IndexedSeq(shard1))) shouldEqual Some(region1)
    }

    "takeShards" in {
      val strategy = RebalanceAllStrategy[Id]().takeShards(2)
      val allocation = Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq(shard2, shard3),
      )
      strategy.rebalance(allocation, Set.empty) shouldEqual List(shard1, shard2)
      strategy.rebalance(allocation, Set(shard4)) shouldEqual List(shard1)
    }

    "threshold" in {
      val strategy = RebalanceAllStrategy[Id]().rebalanceThreshold(2)
      strategy.rebalance(
        Map(region1 -> IndexedSeq(shard1), region2 -> IndexedSeq(shard2, shard3)),
        Set.empty,
      ) shouldEqual List(shard1, shard2, shard3)

      strategy.rebalance(
        Map(region1 -> IndexedSeq(shard1)),
        Set.empty,
      ) shouldEqual Nil
    }

    "track unallocated" in {
      val maxSimulations = 2
      val test = for {
        strategy <- RebalanceAllStrategy[SyncIO]()
          .takeShards(SyncIO.pure(maxSimulations))
          .withTrackUnallocated
        initAllocation = Map(
          region1 -> IndexedSeq(shard1, shard2, shard3, shard4, shard5),
        )
        rebalanceInitial <- strategy.rebalance(initAllocation, Set.empty)
        afterShutdownAllocation = Map(
          region1 -> IndexedSeq(shard3, shard4, shard5),
        )
        rebalanceAfterShutdown <- strategy.rebalance(afterShutdownAllocation, Set.empty)
        _ <- strategy.allocate(region2, shard1, afterShutdownAllocation)
        afterAllocateAllocation = Map(
          region1 -> IndexedSeq(shard3, shard4, shard5),
          region2 -> IndexedSeq(shard1),
        )
        rebalanceAfterAllocation <- strategy.rebalance(
          afterAllocateAllocation,
          Set.empty,
        )
      } yield {
        rebalanceInitial should have size 2
        rebalanceAfterShutdown should have size 0
        rebalanceAfterAllocation should have size 1
      }
      test.unsafeRunSync()
    }

    "least shards" in {
      val strategy = LeastShardsStrategy[Id]()
      strategy.allocate(
        region1,
        shard2,
        Map(
          region1 -> IndexedSeq(shard1),
          region2 -> IndexedSeq.empty,
        ),
      ) shouldEqual Some(region2)

      strategy.allocate(
        region1,
        shard1,
        Map(
          region1 -> IndexedSeq.empty,
          region2 -> IndexedSeq.empty,
        ),
      ) shouldEqual None

      strategy.allocate(
        region1,
        shard3,
        Map(
          region1 -> IndexedSeq(shard1),
          region2 -> IndexedSeq(shard2),
        ),
      ) shouldEqual None

      val region = strategy.allocate(
        region1,
        shard2,
        Map(
          region1 -> IndexedSeq(shard1),
          region2 -> IndexedSeq.empty,
          region3 -> IndexedSeq.empty,
        ),
      ).get

      Set(region2, region3) should contain(region)

      strategy.rebalance(Map.empty, Set.empty) shouldEqual Nil

      strategy.rebalance(Map(region1 -> IndexedSeq(shard1)), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(region1 -> IndexedSeq(shard1, shard2)), Set.empty) shouldEqual Nil

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(shard1),
          region2 -> IndexedSeq.empty,
        ),
        Set.empty,
      ) shouldEqual Nil

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(shard1),
          region2 -> IndexedSeq(shard2),
        ),
        Set.empty,
      ) shouldEqual Nil

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(shard1),
          region2 -> IndexedSeq(shard2, shard3),
        ),
        Set.empty,
      ) shouldEqual Nil

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(shard1),
          region2 -> IndexedSeq(shard2, shard3, shard4),
        ),
        Set.empty,
      ) shouldEqual List(shard4)

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(shard1),
          region2 -> IndexedSeq(shard2, shard3, shard4, shard5),
        ),
        Set.empty,
      ) shouldEqual List(shard5)

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(shard2, shard3),
          region3 -> IndexedSeq(shard4, shard5),
        ),
        Set.empty,
      ) shouldEqual List(shard3)

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(shard2, shard3, shard4),
          region3 -> IndexedSeq(shard5, shard6, shard7),
        ),
        Set.empty,
      ) shouldEqual List(shard4, shard7)

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(shard2, shard3, shard4),
          region3 -> IndexedSeq(shard5, shard6, shard7, shard8),
        ),
        Set.empty,
      ) shouldEqual List(shard4, shard8)

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(),
          region3 -> IndexedSeq(),
          region4 -> IndexedSeq(shard1, shard2),
          region5 -> IndexedSeq(shard3, shard4, shard5),
        ),
        Set.empty,
      ) shouldEqual List(shard5, shard2, shard4)
    }

    "least shards, filterRegions, threshold, takeShards" in {
      val strategy = LeastShardsStrategy[Id]()
        .rebalanceThreshold(2)
        .filterRegions(_ != region1)
        .takeShards(1)

      strategy.allocate(region1, shard1, Map(region1 -> IndexedSeq())) shouldEqual None
      strategy.allocate(region2, shard1, Map(region2 -> IndexedSeq())) shouldEqual None
      strategy.allocate(region1, shard1, Map(region2 -> IndexedSeq())) shouldEqual Some(region2)
      strategy.allocate(
        region1,
        shard1,
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(),
          region3 -> IndexedSeq(),
        ),
      ) shouldEqual Some(region2)

      strategy.allocate(
        region1,
        shard1,
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(),
          region2 -> IndexedSeq(),
        ),
      ) shouldEqual Some(region2)

      strategy.allocate(
        region3,
        shard1,
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(),
          region3 -> IndexedSeq(),
        ),
      ) shouldEqual None

      strategy.allocate(
        region1,
        shard1,
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(shard2),
          region3 -> IndexedSeq(),
        ),
      ) shouldEqual Some(region3)

      strategy.allocate(
        region1,
        shard1,
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(shard2, shard3),
          region3 -> IndexedSeq(),
        ),
      ) shouldEqual Some(region3)

      strategy.allocate(
        region1,
        shard1,
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(shard2, shard3),
          region3 -> IndexedSeq(shard4),
        ),
      ) shouldEqual Some(region3)

      strategy.rebalance(Map(region1 -> IndexedSeq()), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(region1 -> IndexedSeq(shard1)), Set.empty) shouldEqual Nil

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(shard1),
          region2 -> IndexedSeq(shard2),
        ),
        Set.empty,
      ) shouldEqual List(shard1)

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(shard1),
          region2 -> IndexedSeq(shard2),
        ),
        Set(shard3),
      ) shouldEqual Nil

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(shard1),
          region3 -> IndexedSeq(shard2),
        ),
        Set.empty,
      ) shouldEqual Nil

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(shard1),
          region3 -> IndexedSeq(shard2, shard3),
        ),
        Set.empty,
      ) shouldEqual Nil

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(shard1),
          region3 -> IndexedSeq(shard2, shard3, shard4),
        ),
        Set.empty,
      ) shouldEqual Nil

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(),
          region3 -> IndexedSeq(shard1, shard2),
        ),
        Set.empty,
      ) shouldEqual Nil

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(),
          region3 -> IndexedSeq(shard1, shard2, shard3),
        ),
        Set.empty,
      ) shouldEqual Nil

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(),
          region3 -> IndexedSeq(shard1, shard2, shard3, shard4),
        ),
        Set.empty,
      ) shouldEqual List(shard4)

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(),
          region3 -> IndexedSeq(shard1, shard2, shard3, shard4, shard5),
        ),
        Set.empty,
      ) shouldEqual List(shard5)

      strategy.rebalance(
        Map(
          region1 -> IndexedSeq(),
          region2 -> IndexedSeq(),
          region3 -> IndexedSeq(shard1, shard2, shard3, shard4, shard5),
        ),
        Set(shard6),
      ) shouldEqual Nil
    }
  }

  val region1 = newRegion()
  val region2 = newRegion()
  val region3 = newRegion()
  val region4 = newRegion()
  val region5 = newRegion()

  val shard1 = "shard1"
  val shard2 = "shard2"
  val shard3 = "shard3"
  val shard4 = "shard4"
  val shard5 = "shard5"
  val shard6 = "shard6"
  val shard7 = "shard7"
  val shard8 = "shard8"

  def newRegion() = RegionOf(actorSystem)
}
