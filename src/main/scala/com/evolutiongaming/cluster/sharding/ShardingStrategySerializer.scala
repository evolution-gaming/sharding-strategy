package com.evolutiongaming.cluster.sharding

import java.nio.ByteBuffer

import akka.actor.{Address, ExtendedActorSystem}
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import java.nio.charset.StandardCharsets.UTF_8

class ShardingStrategySerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {

  private lazy val serialization = SerializationExtension(system)

  def identifier: Int = 730771473

  private val ManifestOld = "AdaptiveStrategy.Key"
  private val ManifestNew = "A"

  def manifest(x: AnyRef): String = {
    x match {
      case _: AdaptiveStrategy.Key => ManifestOld
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
      case ManifestOld => strategyKeyFromBinary(bytes)
      case ManifestNew => strategyKeyFromBinary(bytes)
      case _           => sys.error(s"Cannot deserialize message for manifest $manifest in ${ getClass.getName }")
    }
  }

  private def strategyKeyFromBinary(bytes: Array[Byte]) = {
    val buffer = ByteBuffer.wrap(bytes)
    val identifier = buffer.getInt()
    val manifest = readStr(buffer)
    val bytesAddress = readBytes(buffer)
    val anyRef =
      if (manifest.nonEmpty) serialization.deserialize(bytesAddress, identifier, manifest).get
      else serialization.deserialize(bytesAddress, identifier, classOf[Address].getName).get
    val address = anyRef.asInstanceOf[Address]
    val shard = readStr(buffer)
    AdaptiveStrategy.Key(address, shard)
  }

  private def strategyKeyToBinary(x: AdaptiveStrategy.Key) = {
    val address = x.address
    val serializer = serialization.findSerializerFor(address)
    val bytesAddress = serializer.toBinary(address)
    val manifest = serializer match {
      case serializer: SerializerWithStringManifest => serializer.manifest(address)
      case _ if serializer.includeManifest          => address.getClass.getName
      case _                                        => ""
    }
    val bytesManifest = manifest.getBytes(UTF_8)
    val bytesShard = x.shard.getBytes(UTF_8)
    val buffer = ByteBuffer.allocate(4 + 4 + 4 + 4 + bytesManifest.length + bytesAddress.length + bytesShard.length)
    buffer.putInt(serializer.identifier)
    buffer.putInt(bytesManifest.length)
    buffer.put(bytesManifest)
    buffer.putInt(bytesAddress.length)
    buffer.put(bytesAddress)
    buffer.putInt(bytesShard.length)
    buffer.put(bytesShard)
    buffer.array()
  }

  private def readStr(buffer: ByteBuffer) = {
    val bytes = readBytes(buffer)
    new String(bytes, UTF_8)
  }

  private def readBytes(buffer: ByteBuffer) = {
    val length = buffer.getInt()
    val bytes = new Array[Byte](length)
    buffer.get(bytes)
    bytes
  }
}

