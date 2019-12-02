package com.evolutiongaming.cluster.sharding

import akka.actor.{Address, ExtendedActorSystem}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ShardingStrategySerializerSpec extends AnyFunSuite with ActorSpec with Matchers {

  private val address = Address("", "", "127.0.0.1", 2552)
  private val extendedActorSystem = actorSystem.asInstanceOf[ExtendedActorSystem]
  private val serializer = new ShardingStrategySerializer(extendedActorSystem)
  private val adaptiveStrategyKey = AdaptiveStrategy.Key(address, "shard")

  test("identifier") {
    serializer.identifier shouldEqual 730771473
  }

  test("manifest") {
    serializer.manifest(adaptiveStrategyKey) shouldEqual "AdaptiveStrategy.Key"
  }

  test("toBinary & fromBinary") {
    val manifest = serializer.manifest(adaptiveStrategyKey)
    val bytes = serializer.toBinary(adaptiveStrategyKey)
    serializer.fromBinary(bytes, manifest) shouldEqual adaptiveStrategyKey
  }
}
