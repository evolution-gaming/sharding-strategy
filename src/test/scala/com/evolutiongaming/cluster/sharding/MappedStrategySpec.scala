package com.evolutiongaming.cluster.sharding

import akka.actor.{Actor, Address, Props}
import cats.Id
import org.scalatest.{FunSuite, Matchers}

import scala.collection.immutable.IndexedSeq

class MappedStrategySpec extends FunSuite with ActorSpec with Matchers {

  private val address = Address("", "", "127.0.0.1", 2552)
  private val addressOf = AddressOf.const(address)
  private val region = {
    def actor() = new Actor {
      def receive = PartialFunction.empty
    }

    val props = Props(actor())
    system.actorOf(props)
  }

  private val shard = "shard"

  private val mapping = new MappedStrategy.Mapping[Id] {
    def get(shard: Shard) = Some(address)
    def set(shard: Shard, address: Address) = {}
  }

  private val strategy = MappedStrategy[Id](mapping, addressOf)

  test("allocate") {
    val allocation = Map((region, IndexedSeq(shard)))
    strategy.allocate(region, shard, allocation) shouldEqual Some(region)
  }

  test("rebalance") {
    val allocation = Map((region, IndexedSeq(shard)))
    strategy.rebalance(allocation, Set.empty) shouldEqual List.empty
  }
}
