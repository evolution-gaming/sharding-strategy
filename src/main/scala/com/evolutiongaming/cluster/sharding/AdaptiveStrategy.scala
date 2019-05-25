package com.evolutiongaming.cluster.sharding

import akka.actor._
import akka.cluster.ddata.Replicator.{WriteConsistency, WriteLocal}
import akka.cluster.ddata._
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.ExtractShardId
import cats.effect.concurrent.Ref
import cats.effect.{Resource, Sync}
import cats.implicits._
import cats.{Applicative, ~>}
import com.evolutiongaming.catshelper._
import com.evolutiongaming.cluster.ddata.SafeReplicator
import com.evolutiongaming.cluster.sharding.AdaptiveStrategy.Counters

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try

/**
  * Entity related messages from clients counted and an entity shard reallocated to a node
  * which receives most of client messages for the corresponding entity
  */

object AdaptiveStrategy {

  type Weight = Int

  type MsgWeight = ShardRegion.Msg => Weight

  object MsgWeight {
    lazy val Increment: MsgWeight = _ => 1
  }


  def apply(
    rebalanceThresholdPercent: Int,
    addressOf: AddressOf,
    counters: Counters[Try]
  ): ShardingStrategy = {

    def toAddresses(current: Allocation) = current.keySet map addressOf.apply

    // access from a non-home node is counted twice - on the non-home node and on the home node
    // so, to get correct value for the home counter we need to deduct the sum of other non-home counters from it
    def fix(counters: Map[Address, BigInt], home: Option[Address]): Map[Address, BigInt] = {

      def fix(home: Address, counter: BigInt) = {
        val nonHomeSum = counters.foldLeft(BigInt(0)) { case (sum, (address, counter)) =>
          if (address == home) sum
          else sum + counter
        }

        if (counter <= nonHomeSum) counters
        else {
          val fixed = counter - nonHomeSum
          counters + (home -> fixed)
        }
      }

      home match {
        case Some(home) => counters get home map { counter => fix(home, counter) } getOrElse counters
        case None       =>
          if (counters.isEmpty) counters
          else {
            val (homeGuess, counter) = counters maxBy { case (_, counter) => counter }
            fix(homeGuess, counter)
          }
      }
    }

    val toRebalance = TrieMap.empty[Shard, Address] // TODO

    new ShardingStrategy {

      def allocate(requester: Region, shard: Shard, current: Allocation) = {
        val addresses = toAddresses(current)

        val address = toRebalance.get(shard) filter addresses.contains orElse {

          val zero = (BigInt(0), List.empty[Address])
          val counters1 = {
            val counters1 = counters.get(shard, addresses).get
            fix(counters1, None)
          }

          val (_, maxAddresses) = counters1.foldLeft(zero) { case ((max, maxAddresses), (address, counter)) =>
            if (counter > max) (counter, address :: Nil)
            else if (counter == max) (counter, address :: maxAddresses)
            else (max, maxAddresses)
          }
          if (current.size == maxAddresses.size) None
          else {
            val requesterAddress = addressOf(requester)
            if (maxAddresses contains requesterAddress) Some(requesterAddress)
            else maxAddresses.headOption
          }
        }

        val region = for {
          address <- address
          region <- current.keys find { region => addressOf(region) == address }
        } yield region


        if (region.isDefined) {
          counters.reset(shard, addresses)
          toRebalance.remove(shard)
        }
        region
      }

      def rebalance(current: Allocation, inProgress: Set[Shard]) = {
        val addresses = toAddresses(current)

        def rebalance(shard: Shard, home: Address) = {
          val counters1 = {
            val counters1 = counters.get(shard, addresses).get
            fix(counters1, Some(home))
          }

          val sum = counters1.foldLeft(BigInt(0)) { case (sum, (_, counter)) => sum + counter }
          if (sum == 0) None
          else {
            val ratios = {
              val multiplier = 100.0 / sum.toDouble
              counters1 map { case (address, counter) =>
                val ratio = multiplier * counter.toDouble
                (address, ratio)
              }
            }
            val homeRatio = ratios.collectFirst { case (`home`, value) => value.toDouble } getOrElse 0.0
            val (maxAddress, maxRatio) = ratios.maxBy { case (_, value) => value.toDouble }

            if (maxAddress != home && maxRatio > (homeRatio + rebalanceThresholdPercent)) {
              Some(maxAddress)
            } else {
              None
            }
          }
        }

        val result = for {
          (region, shards) <- current
          address = addressOf(region)
          shard <- shards
          address <- rebalance(shard, address)
        } yield {
          toRebalance.put(shard, address)
          shard
        }

        result.toList.sorted
      }
    }
  }


  def extractShardId(
    counters: Counters[Try],
    extractShardId: ExtractShardId,
    msgWeight: MsgWeight
  ): ExtractShardId = {

    case msg: ShardRegion.StartEntity => extractShardId(msg)

    case msg =>
      val weight = msgWeight(msg)
      val shardId = extractShardId(msg)
      if (weight > 0) counters.increase(shardId, weight)
      shardId
  }


  trait Counters[F[_]] {

    def increase(shard: Shard, weight: Weight): F[Unit]

    def reset(shard: Shard, addresses: Set[Address]): F[Unit]

    def get(shard: Shard, addresses: Set[Address]): F[Map[Address, BigInt]]
  }

  object Counters {

    def empty[F[_] : Applicative]: Counters[F] = new Counters[F] {

      def increase(shard: Shard, weight: Weight) = ().pure[F]

      def reset(shard: Shard, addresses: Set[Address]) = ().pure[F]

      def get(shard: Shard, addresses: Set[Address]) = Map.empty[Address, BigInt].pure[F]
    }


    def of[F[_] : Sync : LogOf : FromFuture : ToFuture](
      typeName: String,
      replicatorRef: ActorRef)(implicit
      system: ActorSystem
    ): Resource[F, Counters[F]] = {

      implicit val consistency = WriteLocal
      implicit val executor = system.dispatcher
      val dataKey = PNCounterMapKey[Key](s"AdaptiveStrategy-$typeName")
      val replicator = SafeReplicator(dataKey, 1.minute, replicatorRef)
      val selfUniqueAddress = Sync[F].delay { DistributedData(system).selfUniqueAddress }
      val log = LogOf[F].apply(AdaptiveStrategy.getClass)
      for {
        log0              <- Resource.liftF(log)
        log                = log0 prefixed typeName
        selfUniqueAddress <- Resource.liftF(selfUniqueAddress)
        result            <- of(replicator, log, selfUniqueAddress)
      } yield result
    }


    def of[F[_] : Sync](
      replicator: SafeReplicator[F, PNCounterMap[Key]],
      log: Log[F],
      selfUniqueAddress: SelfUniqueAddress)(implicit
      consistency: WriteConsistency,
      executor: ExecutionContext,
      refFactory: ActorRefFactory
    ): Resource[F, Counters[F]] = {
      
      val address = selfUniqueAddress.uniqueAddress.address
      val counters = Ref[F].of(Map.empty[Key, BigInt])

      for {
        counters  <- Resource.liftF(counters)
        onChanged  = (data: PNCounterMap[Key]) => counters.set(data.entries)
        _         <- replicator.subscribe(().pure[F], onChanged)
      } yield {
        
        def update(onSuccess: => String, onFailure: => String)(modify: PNCounterMap[Key] => PNCounterMap[Key]): F[Unit] = {
          val result = for {
            result <- replicator.update { value => modify(value getOrElse PNCounterMap.empty) }
            _      <- log.debug(onSuccess)
          } yield {
            result
          }

          result.handleErrorWith { error =>
            for {
              _ <- log.warn(s"$onFailure: $error")
              a <- error.raiseError[F, Unit]
            } yield a
          }
        }

        new Counters[F] {

          def increase(shard: Shard, weight: Weight) = {
            val key = Key(address, shard)
            update(s"incremented $shard by $weight", s"failed to increment $shard by $weight") { data =>
              data.increment(selfUniqueAddress, key, weight.toLong)
            }
          }

          def reset(shard: Shard, addresses: Set[Address]) = {
            update(s"reset $shard", s"failed to reset $shard") { data =>
              addresses.foldLeft(data) { (data, address) =>
                val key = Key(address, shard)
                val value = data.get(key)
                value.fold(data) { value => data.decrement(selfUniqueAddress, key, value.toLong) }
              }
            }
          }

          def get(shard: Shard, addresses: Set[Address]) = {
            for {
              counters <- counters.get
            } yield {
              val result = for {
                address <- addresses
                key      = Key(address, shard)
                counter  = counters.getOrElse(key, BigInt(0))
              } yield {
                (address, counter)
              }
              result.toMap
            }
          }
        }
      }
    }


    implicit class CounterOps[F[_]](val self: Counters[F]) extends AnyVal {

      def mapK[G[_]](f: F ~> G): Counters[G] = new Counters[G] {

        def increase(shard: Shard, weight: Weight) = f(self.increase(shard, weight))

        def reset(shard: Shard, addresses: Set[Address]) = f(self.reset(shard, addresses))

        def get(shard: Shard, addresses: Set[Address]) = f(self.get(shard, addresses))
      }
    }
  }


  trait Ext extends Extension {

    def replicatorRef: ActorRef
  }

  object Ext extends ExtensionId[Ext] {

    def createExtension(system: ExtendedActorSystem) = new Ext {
      val replicatorRef = {
        val settings = ReplicatorSettings(system)
        val props = Replicator.props(settings)
        system.actorOf(props, "adaptiveStrategyReplicator")
      }
    }
  }

  final case class Key(address: Address, shard: Shard)
}


object AdaptiveStrategyAndExtractShardId {

  def apply[F[_] : Sync : LogOf : FromFuture : ToFuture : ToTry](
    typeName: String,
    rebalanceThresholdPercent: Int,
    msgWeight: AdaptiveStrategy.MsgWeight,
    extractShardId: ExtractShardId)(implicit
    actorSystem: ActorSystem
  ): Resource[F, (ShardingStrategy, ExtractShardId)] = {

    val toTry = new (F ~> Try) {
      def apply[A](fa: F[A]) = ToTry[F].apply(fa) // TODO remove
    }

    val ext = Sync[F].delay { AdaptiveStrategy.Ext(actorSystem) }

    val addressOf = Sync[F].delay { AddressOf(actorSystem) }

    for {
      addressOf <- Resource.liftF(addressOf)
      ext       <- Resource.liftF(ext)
      counters  <- Counters.of[F](typeName, ext.replicatorRef)
    } yield {
      val countersTry = counters.mapK(toTry)
      val adaptiveExtractShardId = AdaptiveStrategy.extractShardId(countersTry, extractShardId, msgWeight)
      val adaptiveStrategy = AdaptiveStrategy(
        rebalanceThresholdPercent = rebalanceThresholdPercent,
        counters = countersTry,
        addressOf = addressOf)
      (adaptiveStrategy, adaptiveExtractShardId)
    }
  }
}