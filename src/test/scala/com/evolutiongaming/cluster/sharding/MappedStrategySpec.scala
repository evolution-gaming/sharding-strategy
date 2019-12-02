package com.evolutiongaming.cluster.sharding

import akka.actor.Address
import cats.Id
import cats.implicits._

import scala.collection.immutable.IndexedSeq
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MappedStrategySpec extends AnyFunSuite with ActorSpec with Matchers {

  private val shard = "shard"
  private val region1 = RegionOf(actorSystem)
  private val region2 = RegionOf(actorSystem)
  private val address1 = Address("", "", "127.0.0.1", 2552)
  private val address2 = Address("", "", "127.0.0.2", 2552)

  private def strategyOf(address: Option[Address]) = {
    val addressOf = new AddressOf {
      def apply(region: Region) = region match {
        case `region1` => address1
        case `region2` => address2
        case _         => region.path.address
      }
    }
    val mapping = new MappedStrategy.Mapping[Id] {
      def get(shard: Shard) = address
      def set(shard: Shard, address: Address) = {}
    }
    MappedStrategy[Id](mapping, addressOf)
  }

  for {
    (shard, allocation, address, expected) <- List(
      (shard, Map((region1, IndexedSeq.empty[Shard]), (region2, IndexedSeq.empty[Shard])), none[Address], none[Region]),
      (shard, Map((region1, IndexedSeq.empty[Shard]), (region2, IndexedSeq.empty[Shard])), address1.some, region1.some),
      (shard, Map((region1, IndexedSeq(shard)),       (region2, IndexedSeq.empty[Shard])), none[Address], none[Region]),
      (shard, Map((region1, IndexedSeq(shard)),       (region2, IndexedSeq.empty[Shard])), address1.some, region1.some)
    )
  } {
    test(s"allocate shard: $shard, address: $address, allocation: $allocation") {
      val strategy = strategyOf(address)
      strategy.allocate(region1, shard, allocation) shouldEqual expected
    }
  }

  for {
    (allocation, address, expected) <- List(
      (Map((region1, IndexedSeq.empty[Shard])),                        none[Address], List.empty[Shard]),
      (Map((region1, IndexedSeq.empty[Shard])),                        address1.some, List.empty[Shard]),
      (Map((region1, IndexedSeq(shard))),                              none[Address], List.empty[Shard]),
      (Map((region1, IndexedSeq(shard))),                              address1.some, List.empty[Shard]),
      (Map((region1, IndexedSeq.empty[Shard])),                        address2.some, List.empty[Shard]),
      (Map((region1, IndexedSeq(shard)), (region2, IndexedSeq.empty)), address2.some, List(shard))
    )
  } {
    test(s"rebalance address: $address, allocation: $allocation") {
      val strategy = strategyOf(address)
      strategy.rebalance(allocation, Set.empty) shouldEqual expected
    }
  }
}
