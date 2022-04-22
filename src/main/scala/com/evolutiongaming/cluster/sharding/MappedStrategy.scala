package com.evolutiongaming.cluster.sharding


import akka.actor.{ActorRef, ActorRefFactory, ActorSystem, Address, ExtendedActorSystem, Extension, ExtensionId}
import akka.cluster.ddata.Replicator.{ReadConsistency, ReadLocal, WriteConsistency, WriteLocal}
import akka.cluster.ddata._
import cats.effect.kernel.Ref
import cats.effect.{Resource, Sync}
import cats.implicits._
import cats.effect.syntax.resource._
import cats.{FlatMap, Parallel, ~>}
import com.evolutiongaming.catshelper.{FromFuture, ToFuture}
import com.evolutiongaming.cluster.ddata.SafeReplicator

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.control.NoStackTrace


object MappedStrategy {

  def of[F[_] : Sync : Parallel : FromFuture : ToFuture](
    typeName: String)(implicit
    actorSystem: ActorSystem
  ): Resource[F, ShardingStrategy[F]] = {

    val ext = Sync[F].delay { Ext(actorSystem) }
    val addressOf = Sync[F].delay { AddressOf(actorSystem) }
    for {
      ext       <- ext.toResource
      addressOf <- addressOf.toResource
      mapping   <- Mapping.of[F](typeName, ext.replicatorRef)
    } yield {
      apply(mapping, addressOf)
    }
  }

  def apply[F[_] : FlatMap : Parallel](mapping: Mapping[F], addressOf: AddressOf): ShardingStrategy[F] = {

    def regionByAddress(address: Address, current: Allocation) = {
      current.keys find { region => addressOf(region) == address }
    }

    new ShardingStrategy[F] {

      def allocate(requester: Region, shard: Shard, current: Allocation) = {
        for {
          address <- mapping get shard
        } yield for {
          address <- address
          region  <- regionByAddress(address, current)
        } yield {
          region
        }
      }

      def rebalance(current: Allocation, inProgress: Set[Shard]) = {
        val shards = for {
          (region, shards) <- current
          shard            <- shards
        } yield for {
          address <- mapping get shard
        } yield for {
//          address <- address if addressOf(region) != address && regionByAddress(address, current).isDefined
          address <- address
          _ = address if addressOf(region) != address && regionByAddress(address, current).isDefined
        } yield {
          shard
        }

        for {
          shards <- Parallel.parSequence(shards.toList)
        } yield {
          shards.flatten.sorted
        }
      }
    }
  }


  trait Mapping[F[_]] {

    def get(shard: Shard): F[Option[Address]]

    def set(shard: Shard, address: Address): F[Unit]
  }

  object Mapping {

    def of[F[_] : Sync : FromFuture : ToFuture](
      typeName: String,
      replicatorRef: ActorRef)(implicit
      actorSystem: ActorSystem
    ): Resource[F, Mapping[F]] = {

      implicit val executor = actorSystem.dispatcher
      implicit val writeConsistency = WriteLocal
      implicit val readConsistency = ReadLocal
      val selfUniqueAddress = Sync[F].delay { DistributedData(actorSystem).selfUniqueAddress }
      val dataKey = LWWMapKey[Shard, Address](s"MappedStrategy-$typeName")
      val replicator = SafeReplicator(dataKey, 1.minute, replicatorRef)

      for {
        selfUniqueAddress <- selfUniqueAddress.toResource
        result            <- of(replicator, selfUniqueAddress)
      } yield result
    }

    def of[F[_]: Sync](
      replicator: SafeReplicator[F, LWWMap[Shard, Address]],
      selfUniqueAddress: SelfUniqueAddress)(implicit
      writeConsistency: WriteConsistency,
      readConsistency: ReadConsistency,
      executor: ExecutionContext,
      refFactory: ActorRefFactory
    ): Resource[F, Mapping[F]] = {

      val cache = Ref[F].of(Map.empty[Shard, Address])

      for {
        cache     <- cache.toResource
        onChanged  = (data: LWWMap[Shard, Address]) => cache.set(data.entries)
        _         <- replicator.subscribe(().pure[F], onChanged)
      } yield {
        new Mapping[F] {

          def get(shard: Shard) = {
            for {
              cache <- cache.get
            } yield {
              cache.get(shard)
            }
          }

          def set(shard: Shard, address: Address) = {

            def mapped(map: Map[Shard, Address]) = map get shard contains address

            def empty = LWWMap.empty[Shard, Address]

            def data = {
              for {
                result <- replicator.get
              } yield {
                result getOrElse empty
              }
            }

            def update(data: LWWMap[Shard, Address], cache: Map[Shard, Address]) = {
              if (mapped(data.entries) && mapped(cache)) ().pure[F]
              else for {
                _ <- replicator.update { data => (data getOrElse empty).put(selfUniqueAddress, shard, address) }
                _ <- replicator.flushChanges
              } yield {}
            }

            val result = for {
              data    <- data
              cache   <- cache.get
              result <- update(data, cache)
            } yield result

            result.handleErrorWith { error =>
              MappingError(shard, address, s"failed to map $shard to $address: $error", error).raiseError[F, Unit]
            }
          }
        }
      }
    }


    implicit class MappingOps[F[_]](val self: Mapping[F]) extends AnyVal {

      def mapK[G[_]](f: F ~> G): Mapping[G] = new Mapping[G] {

        def get(shard: Shard) = f(self.get(shard))

        def set(shard: Shard, address: Address) = f(self.set(shard, address))
      }
    }
  }


  final case class MappingError(
    shard: Shard,
    address: Address,
    msg: String,
    cause: Throwable
  ) extends RuntimeException(msg, cause) with NoStackTrace


  trait Ext extends Extension {

    def replicatorRef: ActorRef
  }

  object Ext extends ExtensionId[Ext] {

    def createExtension(system: ExtendedActorSystem) = new Ext {
      val replicatorRef = {
        val settings = ReplicatorSettings(system)
        val props = Replicator.props(settings)
        system.actorOf(props, "mappedStrategyReplicator")
      }
    }
  }
}