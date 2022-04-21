package com.evolutiongaming.cluster.sharding

import akka.actor.Address
import cats.arrow.FunctionK
import cats.effect.{IO, Ref}
import com.evolutiongaming.cluster.sharding.AdaptiveStrategy.Counters
import com.evolutiongaming.cluster.sharding.IOSuite._
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

class AdaptiveStrategySpec extends AsyncFunSuite with ActorSpec with Matchers {

  test("return None when counters are empty") {
    val result = for {
      strategy <- AdaptiveStrategy.of[IO](10, addressOf, Counters.empty[IO])
      region   <- strategy.allocate(region1, shard1, allocation)
    } yield {
      region shouldEqual None
    }
    result.run()
  }

  test("return None if all have same counters") {
    val result = for {
      ref      <- Ref[IO].of(Map.empty[AdaptiveStrategy.Key, BigInt])
      counters  = Counters(address1, ref).mapK(FunctionK.id)
      _        <- ref.update { map =>
        map ++ Map[AdaptiveStrategy.Key, BigInt](
          (AdaptiveStrategy.Key(address1, shard1), 2),
          (AdaptiveStrategy.Key(address2, shard1), 2),
          (AdaptiveStrategy.Key(address3, shard1), 2))
      }
      strategy <- AdaptiveStrategy.of[IO](10, addressOf, counters)
      result1  <- strategy.allocate(region1, shard1, allocation)
      result2  <- strategy.allocate(region2, shard1, allocation)
      result3  <- strategy.allocate(region3, shard1, allocation)
    } yield {
      result1 shouldEqual None
      result2 shouldEqual None
      result3 shouldEqual None
    }
    result.run()
  }

  test("return region with max counters") {
    val result = for {
      ref      <- Ref[IO].of(Map.empty[AdaptiveStrategy.Key, BigInt])
      counters  = Counters(address1, ref)
      strategy <- AdaptiveStrategy.of[IO](10, addressOf, counters)
      _        <- counters.increase(shard1, 1)
      result   <- strategy.allocate(region1, shard1, allocation)
    } yield {
      result shouldEqual Some(region1)
    }
    result.run()
  }

  test("return region decided in rebalance") {
    val result = for {
      ref      <- Ref[IO].of(Map.empty[AdaptiveStrategy.Key, BigInt])
      counters  = Counters(address1, ref)
      strategy <- AdaptiveStrategy.of[IO](10, addressOf, counters)
      _        <- counters.increase(shard2, 2)
      result1  <- strategy.rebalance(allocation, Set.empty)
      result2  <- strategy.allocate(region2, shard2, allocation)
    } yield {
      result1 shouldEqual List(shard2)
      result2 shouldEqual Some(region1)
    }
    result.run()
  }

  test("return requester if it has the max counter") {
    val result = for {
      ref      <- Ref[IO].of(Map.empty[AdaptiveStrategy.Key, BigInt])
      counters  = Counters(address1, ref)
      _        <- ref.update { map =>
        map ++ Map[AdaptiveStrategy.Key, BigInt](
          (AdaptiveStrategy.Key(address1, shard1), 2),
          (AdaptiveStrategy.Key(address2, shard1), 2))
      }
      strategy <- AdaptiveStrategy.of[IO](10, addressOf, counters)
      result   <- strategy.allocate(region1, shard1, allocation)
    } yield {
      result shouldEqual Some(region1)
    }
    result.run()
  }

  test("return region and fix counters") {
    val result = for {
      ref      <- Ref[IO].of(Map.empty[AdaptiveStrategy.Key, BigInt])
      counters  = Counters(address1, ref)
      _        <- ref.update { map =>
        map ++ Map[AdaptiveStrategy.Key, BigInt](
          (AdaptiveStrategy.Key(address1, shard1), 2),
          (AdaptiveStrategy.Key(address2, shard1), 4))
      }
      strategy <- AdaptiveStrategy.of[IO](10, addressOf, counters)
      result   <- strategy.allocate(region1, shard1, allocation)
    } yield {
      result shouldEqual Some(region1)
    }
    result.run()
  }

  test("not reset counters if not allocated") {
    val result = for {
      ref      <- Ref[IO].of(Map.empty[AdaptiveStrategy.Key, BigInt])
      counters  = Counters(address1, ref)
      _        <- ref.update { map =>
        map ++ Map[AdaptiveStrategy.Key, BigInt](
          (AdaptiveStrategy.Key(address1, shard1), 2),
          (AdaptiveStrategy.Key(address2, shard1), 2),
          (AdaptiveStrategy.Key(address3, shard1), 2))
      }
      strategy <- AdaptiveStrategy.of[IO](10, addressOf, counters)
      result1  <- strategy.allocate(region1, shard1, allocation)
      result2  <- counters.get(shard1, Set(address1))
    } yield {
      result1 shouldEqual None
      result2 shouldEqual Map(address1 -> BigInt(2))
    }
    result.run()
  }

  test("reset counters if allocated") {
    val result = for {
      ref      <- Ref[IO].of(Map.empty[AdaptiveStrategy.Key, BigInt])
      counters  = Counters(address1, ref)
      strategy <- AdaptiveStrategy.of[IO](10, addressOf, counters)
      _        <- counters.increase(shard1, 1)
      result1  <- strategy.allocate(region2, shard1, allocation)
      result2  <- counters.get(shard1, Set(address1))
    } yield {
      result1 shouldEqual Some(region1)
      result2 shouldEqual Map(address1 -> BigInt(0))
    }
    result.run()
  }

  test("not return when counters are empty") {
    val counters = Counters.empty[IO].mapK(FunctionK.id)
    val result = for {
      strategy <- AdaptiveStrategy.of[IO](10, addressOf, counters)
      result   <- strategy.rebalance(allocation, Set.empty)
    } yield {
      result shouldEqual Nil
    }
    result.run()
  }

  test("not return shard when home node has the max counter") {
    val result = for {
      ref      <- Ref[IO].of(Map.empty[AdaptiveStrategy.Key, BigInt])
      counters  = Counters(address1, ref)
      strategy <- AdaptiveStrategy.of[IO](10, addressOf, counters)
      _        <- counters.increase(shard1, 100)
      result   <- strategy.rebalance(allocation, Set.empty)
    } yield {
      result shouldEqual Nil
    }
    result.run()
  }

  test("return shard to non-home node has the max counter") {
    val result = for {
      ref      <- Ref[IO].of(Map.empty[AdaptiveStrategy.Key, BigInt])
      counters  = Counters(address1, ref)
      strategy <- AdaptiveStrategy.of[IO](10, addressOf, counters)
      _        <- counters.increase(shard2, 3)
      result   <- strategy.rebalance(allocation, Set.empty)
    } yield {
      result shouldEqual List(shard2)
    }
    result.run()
  }

  test("not return shard when threshold is not reached") {
    val result = for {
      ref      <- Ref[IO].of(Map.empty[AdaptiveStrategy.Key, BigInt])
      counters  = Counters(address1, ref)
      _        <- ref.update { map =>
        map ++ Map[AdaptiveStrategy.Key, BigInt](
          (AdaptiveStrategy.Key(address1, shard1), 3),
          (AdaptiveStrategy.Key(address2, shard1), 2))
      }
      strategy <- AdaptiveStrategy.of[IO](10, addressOf, counters)
      result   <- strategy.rebalance(allocation, Set.empty)
    } yield {
      result shouldEqual List(shard1)
    }
    result.run()
  }


  private def newRegion() = RegionOf(actorSystem)

  private def newAddress(ip: String) = Address("", "", ip, 2552)

  private val region1 = newRegion()
  private val region2 = newRegion()
  private val region3 = newRegion()

  private val address1 = newAddress("127.0.0.1")
  private val address2 = newAddress("127.0.0.2")
  private val address3 = newAddress("127.0.0.3")

  private val shard1 = "shard1"
  private val shard2 = "shard2"
  private val shard3 = "shard3"

  private val addressOf = {
    val addresses = Map(
      region1 -> address1,
      region2 -> address2,
      region3 -> address3)
    new AddressOf {
      def apply(region: Region) = addresses(region)
    }
  }

  private val allocation = Map(
    (region1, IndexedSeq(shard1)),
    (region2, IndexedSeq(shard2)),
    (region3, IndexedSeq(shard3)))
}
