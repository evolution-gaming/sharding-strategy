package com.evolutiongaming.cluster.sharding


import akka.actor.{ActorRef, ActorSystem, Address, ExtendedActorSystem, Extension, ExtensionId}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator.{ReadLocal, WriteLocal}
import akka.cluster.ddata.{LWWMap, LWWMapKey, Replicator, ReplicatorSettings}
import com.evolutiongaming.cluster.ddata.SafeReplicator
import com.evolutiongaming.cluster.ddata.SafeReplicator.{GetFailure, UpdateFailure}
import com.evolutiongaming.safeakka.actor.ActorLog

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.duration._

class MappedStrategy private(
  mapping: MappedStrategy.Mapping,
  toAddress: Region => Address) extends ShardingStrategy {

  def allocate(requesterRef: ActorRef, requester: Region, shard: Shard, current: Allocation) = {
    for {
      address <- mapping get shard
      region <- regionByAddress(address, current)
    } yield region
  }

  def rebalance(current: Allocation, inProgress: Set[Shard]) = {

    val toRebalance = for {
      (region, shards) <- current
      shard <- shards
      address <- mapping get shard
      if toAddress(region) != address && regionByAddress(address, current).isDefined
    } yield shard

    toRebalance.toList.sorted
  }

  private def regionByAddress(address: Address, current: Allocation) = {
    current.keys find { region => toAddress(region) == address }
  }
}

object MappedStrategy {

  def apply(typeName: String)
    (implicit system: ActorSystem): MappedStrategy = {
    val mapping = MappingExtension(system)(typeName)
    val absoluteAddress = AbsoluteAddress(system)
    apply(mapping, (region: Region) => absoluteAddress(region.path.address))
  }

  def apply(mapping: MappedStrategy.Mapping, toAddress: Region => Address): MappedStrategy = {
    new MappedStrategy(mapping, toAddress)
  }

  trait Mapping {
    def get(shard: Shard): Option[Address]
    def set(shard: Shard, address: Address): Unit
  }

  object Mapping {
    def apply(typeName: String, replicatorRef: ActorRef)(implicit system: ActorSystem): Mapping = {
      implicit val cluster = Cluster(system)
      implicit val ec = system.dispatcher
      implicit val writeConsistency = WriteLocal
      implicit val readConsistency = ReadLocal

      val dataKey = LWWMapKey[Shard, Address](s"MappedStrategy-$typeName")
      val replicator = SafeReplicator(dataKey, 30.seconds, replicatorRef)
      val log = ActorLog(system, MappedStrategy.getClass) prefixed typeName

      var cache = Map.empty[Shard, Address]
      replicator.subscribe() { data => cache = data.entries }

      new Mapping {

        def get(shard: Shard): Option[Address] = {
          cache.get(shard)
        }

        def set(shard: Shard, address: Address): Unit = {
          def onFailure = s"failed to map $shard to $address"

          def mapped(map: Map[Shard, Address]) = map get shard contains address

          type Failure = (String, Option[Throwable])

          def empty = LWWMap.empty[Shard, Address]

          def data: Future[Either[Failure, LWWMap[Shard, Address]]] = replicator.get map {
            case Right(result)             => result.asRight
            case Left(GetFailure.NotFound) => empty.asRight
            case Left(result)              => (s"$onFailure: $result", Option.empty[Throwable]).asLeft
          }

          def update(data: LWWMap[Shard, Address]): Future[Either[Failure, Boolean]] = {
            if (mapped(data.entries) && mapped(cache)) {
              Future.successful(false.asRight)
            } else {
              val result = replicator.update { data => (data getOrElse empty) + (shard -> address) }
              result.map {
                case Right(())                                   => true.asRight
                case Left(UpdateFailure.Failure(message, cause)) => (s"$onFailure: $message $cause", Some(cause)).asLeft
                case Left(result)                                => (s"$onFailure: $result", None).asLeft
              }
            }
          }

          val result = for {
            data <- data
            result <- data match {
              case Right(data)   => update(data)
              case Left(failure) => Future.successful(failure.asLeft)
            }
          } yield result

          result foreach {
            case Right(updated)               => if (updated) {
              replicator.flushChanges()
              log.debug(s"mapped $shard to $address")
            }
            case Left((message, Some(cause))) => log.error(message, cause)
            case Left((message, None))        => log.error(message)
          }
          result.failed foreach { failure =>
            log.error(s"$onFailure: $failure", failure)
          }
        }
      }
    }
  }

  class MappingExtension(implicit system: ActorSystem) extends Extension {

    private lazy val replicatorRef = {
      val settings = ReplicatorSettings(system)
      val props = Replicator.props(settings)
      system.actorOf(props, "mappedStrategyReplicator")
    }

    private val cache = TrieMap.empty[String, Mapping]

    def apply(typeName: String): Mapping = {
      cache.getOrElseUpdate(typeName, Mapping(typeName, replicatorRef))
    }
  }

  object MappingExtension extends ExtensionId[MappingExtension] {
    def createExtension(system: ExtendedActorSystem): MappingExtension = new MappingExtension()(system)
  }

  private implicit class EitherOps[A](val self: A) extends AnyVal {
    def asLeft[B]: Either[A, B] = Left(self)
    def asRight[B]: Either[B, A] = Right(self)
  }
}