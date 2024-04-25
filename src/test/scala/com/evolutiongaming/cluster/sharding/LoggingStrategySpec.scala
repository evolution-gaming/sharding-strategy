package com.evolutiongaming.cluster.sharding

import akka.actor.Address
import cats.implicits.*

import scala.collection.immutable.IndexedSeq
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LoggingStrategySpec extends AnyFunSuite with ActorSpec with Matchers {

  private val region = RegionOf(actorSystem)
  private val shard = "shard"
  private val address = Address("", "", "127.0.0.1", 2552)

  private implicit val addressOf: AddressOf = AddressOf.const(address)

  private val log = (msg: () => String) => {
    StateT { state =>
      val state1 = state.copy(msg() :: state.records)
      (state1, ())
    }
  }

  test(s"allocate") {
    val strategy = RemoteNodeStrategy[StateT]
      .filterShards(((_: Shard) => true).pure[StateT])
      .logging(log)
    val allocation = Map((region, IndexedSeq.empty[Shard]))
    val (state, region1) = strategy.allocate(region, shard, allocation).run(State.Empty)
    state shouldEqual State(List("allocate shard to none, requester: 127.0.0.1:2552, current: 127.0.0.1:2552(0): []"))
    region1 shouldEqual None
  }

  test("rebalance") {
    val strategy = RebalanceAllStrategy[StateT]()
      .logging(log)
    val allocation = Map((region, IndexedSeq(shard)))
    val (state, shards) = strategy.rebalance(allocation, Set(shard)).run(State.Empty)
    state shouldEqual State(List("rebalance shard, inProgress(1): [shard], current: 127.0.0.1:2552(1): [shard]"))
    shards shouldEqual List(shard)
  }


  case class State(records: List[String])

  object State {
    val Empty: State = State(List.empty)
  }


  type StateT[A] = cats.data.StateT[cats.Id, State, A]

  object StateT {

    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[cats.Id, State, A](f)
  }
}