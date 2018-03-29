package com.evolutiongaming.cluster.sharding

import akka.actor.{Actor, ActorRef, Address, Props}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable.IndexedSeq

class ShardingStrategySpec extends WordSpec with ActorSpec with Matchers {

  "ShardingStrategy" should {

    "filterRegions" in {

      val stub = new ShardingStrategy {
        def allocate(requesterRef: ActorRef, requester: Region, shard: Shard, current: Allocation) = Some(requester)
        def rebalance(current: Allocation, inProgress: Set[Shard]) = Nil
      }

      val strategy = stub.filterRegions(_ == region1)

      val allocation = Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq(shard2, shard3),
        region3 -> IndexedSeq(shard4))

      strategy.rebalance(allocation, Set.empty) shouldEqual List(shard2, shard3, shard4)
      strategy.rebalance(Map.empty, Set.empty) shouldEqual Nil

      strategy.allocate(ActorRef.noSender, region2, shard2, Map(region2 -> IndexedSeq(shard1))) shouldEqual None
      strategy.allocate(ActorRef.noSender, region1, shard2, Map(region1 -> IndexedSeq(shard1))) shouldEqual Some(region1)
    }

    "takeShards" in {
      val strategy = RebalanceAllStrategy.takeShards(2)
      val allocation = Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq(shard2, shard3))
      strategy.rebalance(allocation, Set.empty) shouldEqual List(shard1, shard2)
      strategy.rebalance(allocation, Set(shard4)) shouldEqual List(shard1)
    }

    "filterByRole" in {
      val stub = new ShardingStrategy {
        def allocate(requesterRef: ActorRef, requester: Region, shard: Shard, current: Allocation) = Some(requester)
        def rebalance(current: Allocation, inProgress: Set[Shard]) = Nil
      }

      def shardRole(shard: Shard): Option[String] = shard match {
        case `shard1` => Some("role1")
        case `shard2` => Some("role2")
        case `shard3` => Some("role3")
        case `shard4` => None
      }

      val address1 = newAddress("127.0.0.1")
      val address2 = newAddress("127.0.0.2")
      val address3 = newAddress("127.0.0.3")

      val clusterMembers = Map(
        address1 -> Set("role1", "role4"),
        address2 -> Set("role2", "role3"),
        address3 -> Set("role4", "role"))

      val toAddress = Map(
        region1 -> address1,
        region2 -> address2,
        region3 -> address3)

      val strategy = stub.filterByRole(shardRole, toAddress, clusterMembers)

      val allocation = Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq(shard2, shard3),
        region3 -> IndexedSeq(shard4))

      val wrongAllocation = Map(
        region1 -> IndexedSeq(shard3),
        region2 -> IndexedSeq(shard4, shard1),
        region3 -> IndexedSeq(shard2))

      val partiallyWrongAllocation = Map(
        region1 -> IndexedSeq(shard3),
        region2 -> IndexedSeq(shard4, shard2),
        region3 -> IndexedSeq(shard1))

      strategy.rebalance(wrongAllocation, Set.empty).sorted shouldEqual List(shard1, shard2, shard3)
      strategy.rebalance(partiallyWrongAllocation, Set.empty).sorted shouldEqual List(shard1, shard3)
      strategy.rebalance(allocation, Set.empty) shouldEqual Nil
      strategy.rebalance(Map.empty, Set.empty) shouldEqual Nil

      strategy.allocate(ActorRef.noSender, region3, shard1, wrongAllocation) shouldEqual Some(region1)
      strategy.allocate(ActorRef.noSender, region3, shard2, wrongAllocation) shouldEqual Some(region2)
      strategy.allocate(ActorRef.noSender, region3, shard3, wrongAllocation) shouldEqual Some(region2)
      strategy.allocate(ActorRef.noSender, region3, shard4, wrongAllocation) shouldEqual Some(region3)
    }

    "threshold" in {
      val strategy = RebalanceAllStrategy.rebalanceThreshold(2)
      strategy.rebalance(
        Map(region1 -> IndexedSeq(shard1), region2 -> IndexedSeq(shard2, shard3)),
        Set.empty) shouldEqual List(shard1, shard2, shard3)

      strategy.rebalance(
        Map(region1 -> IndexedSeq(shard1)),
        Set.empty) shouldEqual Nil
    }

    "least shards" in {
      val strategy = LeastShardsStrategy
      strategy.allocate(ActorRef.noSender, region1, shard2, Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq.empty)) shouldEqual Some(region2)

      strategy.allocate(ActorRef.noSender, region1, shard1, Map(
        region1 -> IndexedSeq.empty,
        region2 -> IndexedSeq.empty)) shouldEqual None

      strategy.allocate(ActorRef.noSender, region1, shard3, Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq(shard2))) shouldEqual None

      val region = strategy.allocate(ActorRef.noSender, region1, shard2, Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq.empty,
        region3 -> IndexedSeq.empty)).get

      Set(region2, region3) should contain(region)

      strategy.rebalance(Map.empty, Set.empty) shouldEqual Nil

      strategy.rebalance(Map(region1 -> IndexedSeq(shard1)), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(region1 -> IndexedSeq(shard1, shard2)), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq.empty), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq(shard2)), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq(shard2, shard3)), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq(shard2, shard3, shard4)), Set.empty) shouldEqual List(shard4)

      strategy.rebalance(Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq(shard2, shard3, shard4, shard5)), Set.empty) shouldEqual List(shard5)

      strategy.rebalance(Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(shard2, shard3),
        region3 -> IndexedSeq(shard4, shard5)), Set.empty) shouldEqual List(shard3)

      strategy.rebalance(Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(shard2, shard3, shard4),
        region3 -> IndexedSeq(shard5, shard6, shard7)), Set.empty) shouldEqual List(shard4, shard7)

      strategy.rebalance(Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(shard2, shard3, shard4),
        region3 -> IndexedSeq(shard5, shard6, shard7, shard8)), Set.empty) shouldEqual List(shard4, shard8)

      strategy.rebalance(Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(),
        region3 -> IndexedSeq(),
        region4 -> IndexedSeq(shard1, shard2),
        region5 -> IndexedSeq(shard3, shard4, shard5)), Set.empty) shouldEqual List(shard5, shard2, shard4)
    }


    "least shards, filterRegions, threshold, takeShards" in {
      val strategy = LeastShardsStrategy
        .rebalanceThreshold(2)
        .filterRegions(_ != region1)
        .takeShards(1)

      strategy.allocate(ActorRef.noSender, region1, shard1, Map(region1 -> IndexedSeq())) shouldEqual None
      strategy.allocate(ActorRef.noSender, region2, shard1, Map(region2 -> IndexedSeq())) shouldEqual None
      strategy.allocate(ActorRef.noSender, region1, shard1, Map(region2 -> IndexedSeq())) shouldEqual Some(region2)
      strategy.allocate(ActorRef.noSender, region1, shard1, Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(),
        region3 -> IndexedSeq())) shouldEqual Some(region2)

      strategy.allocate(ActorRef.noSender, region1, shard1, Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(),
        region2 -> IndexedSeq())) shouldEqual Some(region2)

      strategy.allocate(ActorRef.noSender, region3, shard1, Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(),
        region3 -> IndexedSeq())) shouldEqual None

      strategy.allocate(ActorRef.noSender, region1, shard1, Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(shard2),
        region3 -> IndexedSeq())) shouldEqual Some(region3)

      strategy.allocate(ActorRef.noSender, region1, shard1, Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(shard2, shard3),
        region3 -> IndexedSeq())) shouldEqual Some(region3)

      strategy.allocate(ActorRef.noSender, region1, shard1, Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(shard2, shard3),
        region3 -> IndexedSeq(shard4))) shouldEqual Some(region3)

      strategy.rebalance(Map(region1 -> IndexedSeq()), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(region1 -> IndexedSeq(shard1)), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq(shard2)), Set.empty) shouldEqual List(shard1)

      strategy.rebalance(Map(
        region1 -> IndexedSeq(shard1),
        region2 -> IndexedSeq(shard2)), Set(shard3)) shouldEqual Nil

      strategy.rebalance(Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(shard1),
        region3 -> IndexedSeq(shard2)), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(shard1),
        region3 -> IndexedSeq(shard2, shard3)), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(shard1),
        region3 -> IndexedSeq(shard2, shard3, shard4)), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(),
        region3 -> IndexedSeq(shard1, shard2)), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(),
        region3 -> IndexedSeq(shard1, shard2, shard3)), Set.empty) shouldEqual Nil

      strategy.rebalance(Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(),
        region3 -> IndexedSeq(shard1, shard2, shard3, shard4)), Set.empty) shouldEqual List(shard4)

      strategy.rebalance(Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(),
        region3 -> IndexedSeq(shard1, shard2, shard3, shard4, shard5)), Set.empty) shouldEqual List(shard5)

      strategy.rebalance(Map(
        region1 -> IndexedSeq(),
        region2 -> IndexedSeq(),
        region3 -> IndexedSeq(shard1, shard2, shard3, shard4, shard5)), Set(shard6)) shouldEqual Nil
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

  def newAddress(ip: String) = Address("", "", ip, 2552)

  def newRegion() = {
    def actor = new Actor {
      def receive: Receive = PartialFunction.empty
    }

    val props = Props(actor)
    system.actorOf(props)
  }
}
