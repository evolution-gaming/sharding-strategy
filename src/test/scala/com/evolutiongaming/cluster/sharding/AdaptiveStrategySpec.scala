package com.evolutiongaming.cluster.sharding

import akka.actor.{Actor, Address, Props}
import com.evolutiongaming.cluster.sharding.AdaptiveStrategy._
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.util.{Success, Try}

class AdaptiveStrategySpec extends WordSpec with ActorSpec with Matchers {

  "AdaptiveStrategy" when {

    "allocate" should {

      "return None when counters are empty" in new Scope {
        strategy.allocate(region1, shard1, allocation) shouldEqual None
      }

      "return None if all have same counters" in new Scope {
        countersMap.put(AdaptiveStrategy.Key(address1, shard1), 2)
        countersMap.put(AdaptiveStrategy.Key(address2, shard1), 2)
        countersMap.put(AdaptiveStrategy.Key(address3, shard1), 2)

        strategy.allocate(region1, shard1, allocation) shouldEqual None
        strategy.allocate(region2, shard1, allocation) shouldEqual None
        strategy.allocate(region3, shard1, allocation) shouldEqual None
      }

      "return region with max counters" in new Scope {
        counters.increase(shard1, 1)

        strategy.allocate(region2, shard1, allocation) shouldEqual Some(region1)
      }

      "return region decided in rebalance" in new Scope {
        counters.increase(shard2, 2)
        strategy.rebalance(allocation, Set.empty) shouldEqual List(shard2)
        strategy.allocate(region2, shard2, allocation) shouldEqual Some(region1)
      }

      "return requester if it has the max counter" in new Scope {
        countersMap.put(AdaptiveStrategy.Key(address1, shard1), 2)
        countersMap.put(AdaptiveStrategy.Key(address2, shard1), 2)

        strategy.allocate(region1, shard1, allocation) shouldEqual Some(region1)
      }

      "return region and fix counters" in new Scope {
        countersMap.put(AdaptiveStrategy.Key(address1, shard1), 2)
        countersMap.put(AdaptiveStrategy.Key(address2, shard1), 4)

        strategy.allocate(region1, shard1, allocation) shouldEqual Some(region1)
      }

      "not reset counters if not allocated" in new Scope {
        countersMap.put(AdaptiveStrategy.Key(address1, shard1), 2)
        countersMap.put(AdaptiveStrategy.Key(address2, shard1), 2)
        countersMap.put(AdaptiveStrategy.Key(address3, shard1), 2)

        strategy.allocate(region1, shard1, allocation) shouldEqual None
        counters.get(shard1, Set(address1)) shouldEqual Success(Map(address1 -> BigInt(2)))
      }

      "reset counters if allocated" in new Scope {
        counters.increase(shard1, 1)

        strategy.allocate(region2, shard1, allocation) shouldEqual Some(region1)
        counters.get(shard1, Set(address1)) shouldEqual Success(Map(address1 -> BigInt(0)))
      }
    }

    "rebalance" should {

      "not return when counters are empty" in new Scope {
        strategy.rebalance(allocation, Set.empty) shouldEqual Nil
      }

      "not return shard when home node has the max counter" in new Scope {
        counters.increase(shard1, 100)
        strategy.rebalance(allocation, Set.empty) shouldEqual Nil
      }

      "return shard to non-home node has the max counter" in new Scope {
        counters.increase(shard2, 3)
        strategy.rebalance(allocation, Set.empty) shouldEqual List(shard2)
      }

      "not return shard when threshold is not reached" in new Scope {
        countersMap.put(AdaptiveStrategy.Key(address1, shard1), 3)
        countersMap.put(AdaptiveStrategy.Key(address2, shard1), 2)
        strategy.rebalance(allocation, Set.empty) shouldEqual List(shard1)
      }
    }
  }

  private trait Scope {

    val countersMap = mutable.Map.empty[AdaptiveStrategy.Key, BigInt]

    val counters = new AdaptiveStrategy.Counters[Try] {

      def increase(shard: Shard, weight: Weight) = {
        val key = AdaptiveStrategy.Key(address1, shard)
        val counter = countersMap.getOrElse(key, BigInt(0))
        countersMap.put(key, counter + weight)
        Success(())
      }

      def get(shard: Shard, addresses: Set[Address]) = {
        val result = addresses.map { address =>
          val key = AdaptiveStrategy.Key(address, shard)
          val counter = countersMap.getOrElse(key, BigInt(0))
          (address, counter)
        }.toMap
        Success(result)
      }

      def reset(shard: Shard, addresses: Set[Address]) = {
        val result = addresses.foreach { address =>
          val key = AdaptiveStrategy.Key(address, shard)
          countersMap.remove(key)
        }
        Success(result)
      }
    }

    val strategy = AdaptiveStrategy(10, toAddress, counters)
  }

  def newRegion() = {
    def actor = new Actor {
      def receive: Receive = PartialFunction.empty
    }

    val props = Props(actor)
    system.actorOf(props)
  }

  def newAddress(ip: String) = Address("", "", ip, 2552)

  val region1 = newRegion()
  val region2 = newRegion()
  val region3 = newRegion()

  val address1 = newAddress("127.0.0.1")
  val address2 = newAddress("127.0.0.2")
  val address3 = newAddress("127.0.0.3")

  val shard1 = "shard1"
  val shard2 = "shard2"
  val shard3 = "shard3"

  val toAddress = {
    val addresses = Map(
      region1 -> address1,
      region2 -> address2,
      region3 -> address3)
    new AddressOf {
      def apply(region: Region) = addresses(region)
    }
  }

  val allocation = Map(
    region1 -> IndexedSeq(shard1),
    region2 -> IndexedSeq(shard2),
    region3 -> IndexedSeq(shard3))
}
