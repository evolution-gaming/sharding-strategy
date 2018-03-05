package com.evolutiongaming.cluster.sharding

import java.nio.ByteBuffer

import akka.actor.{Address, ExtendedActorSystem}
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}

class ShardingStrategySerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {

  private lazy val serialization = SerializationExtension(system)

  def identifier: Int = 730771473

  private val StrategyKeyManifest = "AdaptiveStrategy.Key"

  def manifest(x: AnyRef): String = {
    x match {
      case _: AdaptiveStrategy.Key => StrategyKeyManifest
      case _                       => sys.error(s"Cannot serialize message of ${ x.getClass } in ${ getClass.getName }")
    }
  }

  def toBinary(x: AnyRef): Array[Byte] = {
    x match {
      case x: AdaptiveStrategy.Key => strategyKeyToBinary(x)
      case _                       => sys.error(s"Cannot serialize message of ${ x.getClass } in ${ getClass.getName }")
    }
  }

  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    manifest match {
      case StrategyKeyManifest => strategyKeyFromBinary(bytes)
      case _                   => sys.error(s"Cannot deserialize message for manifest $manifest in ${ getClass.getName }")
    }
  }

  private def strategyKeyFromBinary(bytes: Array[Byte]) = {
    val buffer = ByteBuffer.wrap(bytes)
    val address = serialization.deserialize(readBytes(buffer), classOf[Address]).get
    val shard = readStr(buffer)
    AdaptiveStrategy.Key(address, shard)
  }

  private def strategyKeyToBinary(x: AdaptiveStrategy.Key) = {
    val bytesAddress = serialization.serialize(x.address).get
    val bytesShard = x.shard.getBytes("UTF-8")
    val buffer = ByteBuffer.allocate(4 + 4 + bytesAddress.length + bytesShard.length)
    buffer.putInt(bytesAddress.length)
    buffer.put(bytesAddress)
    buffer.putInt(bytesShard.length)
    buffer.put(bytesShard)
    buffer.array()
  }

  private def readStr(buffer: ByteBuffer) = {
    val bytes = readBytes(buffer)
    new String(bytes, "UTF-8")
  }

  private def readBytes(buffer: ByteBuffer) = {
    val length = buffer.getInt()
    val bytes = new Array[Byte](length)
    buffer.get(bytes)
    bytes
  }
}

