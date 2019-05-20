package com.evolutiongaming.cluster.sharding

import akka.actor._
import akka.cluster.ddata.Replicator.WriteLocal
import akka.cluster.ddata._
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.ExtractShardId
import com.evolutiongaming.cluster.ddata.SafeReplicator
import com.evolutiongaming.cluster.ddata.SafeReplicator.UpdateFailure
import com.evolutiongaming.safeakka.actor.ActorLog

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._

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
    toAddress: Region => Address,
    counters: AdaptiveStrategy.Counters
  ): ShardingStrategy = {

    def toAddresses(current: Allocation) = current.keySet map toAddress

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

    new ShardingStrategy {

      private val toRebalance = TrieMap.empty[Shard, Address]

      def allocate(requester: Region, shard: Shard, current: Allocation) = {
        val addresses = toAddresses(current)

        val address = toRebalance.get(shard) filter addresses.contains orElse {

          val zero = (BigInt(0), List.empty[Address])
          val counters1 = {
            val counters1 = counters.get(shard, addresses)
            fix(counters1, None)
          }

          val (_, maxAddresses) = counters1.foldLeft(zero) { case ((max, maxAddresses), (address, counter)) =>
            if (counter > max) (counter, address :: Nil)
            else if (counter == max) (counter, address :: maxAddresses)
            else (max, maxAddresses)
          }
          if (current.size == maxAddresses.size) None
          else {
            val requesterAddress = toAddress(requester)
            if (maxAddresses contains requesterAddress) Some(requesterAddress)
            else maxAddresses.headOption
          }
        }

        val region = for {
          address <- address
          region <- current.keys find { region => toAddress(region) == address }
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
            val counters1 = counters.get(shard, addresses)
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
          address = toAddress(region)
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


  def apply(
    typeName: String,
    rebalanceThresholdPercent: Int,
    toAddress: Region => Address)(implicit
    system: ActorSystem
  ): ShardingStrategy = {

    val counters = CountersExtension(system)(typeName)

    AdaptiveStrategy(
      rebalanceThresholdPercent = rebalanceThresholdPercent,
      toAddress = toAddress,
      counters = counters)
  }


  def extractShardId(
    counters: Counters,
    extractShardId: ExtractShardId,
    msgWeight: MsgWeight): ExtractShardId = {

    case msg: ShardRegion.StartEntity => extractShardId(msg)

    case msg =>
      val weight = msgWeight(msg)
      val shardId = extractShardId(msg)
      if (weight > 0) counters.increase(shardId, weight)
      shardId
  }


  trait Counters {
    def increase(shard: Shard, weight: Weight): Unit
    def reset(shard: Shard, addresses: Set[Address]): Unit
    def get(shard: Shard, addresses: Set[Address]): Map[Address, BigInt]
  }

  object Counters {
    def apply(typeName: String, replicatorRef: ActorRef)(implicit system: ActorSystem): Counters = {
      implicit val ec = system.dispatcher
      implicit val consistency = WriteLocal

      val dataKey = PNCounterMapKey[Key](s"AdaptiveStrategy-$typeName")
      val replicator = SafeReplicator(dataKey, 30.seconds, replicatorRef)
      val log = ActorLog(system, AdaptiveStrategy.getClass) prefixed typeName
      val selfUniqueAddress = DistributedData(system).selfUniqueAddress
      val address = selfUniqueAddress.uniqueAddress.address
      var counters = Map.empty[Key, BigInt]
      replicator.subscribe() { value => counters = value.entries }

      def update(onSuccess: => String, onFailure: => String)(modify: PNCounterMap[Key] => PNCounterMap[Key]): Unit = {
        val result = replicator.update { value => modify(value getOrElse PNCounterMap.empty) }
        result foreach {
          case Right(())                               => log.debug(onSuccess)
          case Left(UpdateFailure.Failure(msg, cause)) => log.error(s"$onFailure: $msg", cause)
          case Left(result)                            => log.error(s"$onFailure: $result")
        }
        result.failed foreach { failure =>
          log.error(s"$onFailure: $failure", failure)
        }
      }

      new Counters {

        def increase(shard: Shard, weight: Weight): Unit = {
          val key = Key(address, shard)
          update(s"incremented $shard by $weight", s"failed to increment $shard by $weight") { data =>
            data.increment(selfUniqueAddress, key, weight.toLong)
          }
        }

        def reset(shard: Shard, addresses: Set[Address]): Unit = {
          update(s"reset $shard", s"failed to reset $shard") { data =>
            addresses.foldLeft(data) { case (data, address) =>
              val key = Key(address, shard)
              val value = data.get(key)
              value.fold(data) { value => data.decrement(selfUniqueAddress, key, value.toLong) }
            }
          }
        }

        def get(shard: Shard, addresses: Set[Address]) = {
          val cached = counters // cached against concurrent updates
          addresses.map { address =>
            val key = Key(address, shard)
            val counter = cached.getOrElse(key, BigInt(0))
            (address, counter)
          }.toMap
        }
      }
    }
  }


  class CountersExtension(implicit system: ActorSystem) extends Extension {

    private lazy val replicatorRef = {
      val settings = ReplicatorSettings(system)
      val props = Replicator.props(settings)
      system.actorOf(props, "adaptiveStrategyReplicator")
    }

    private val cache = TrieMap.empty[String, Counters]

    def apply(typeName: String): Counters = {
      cache.getOrElseUpdate(typeName, Counters(typeName, replicatorRef))
    }
  }

  object CountersExtension extends ExtensionId[CountersExtension] {
    def createExtension(system: ExtendedActorSystem): CountersExtension = new CountersExtension()(system)
  }

  final case class Key(address: Address, shard: Shard)
}


object AdaptiveStrategyAndExtractShardId {

  def apply(
    typeName: String,
    rebalanceThresholdPercent: Int,
    msgWeight: AdaptiveStrategy.MsgWeight,
    extractShardId: ExtractShardId)(implicit
    system: ActorSystem
  ): (ShardingStrategy, ExtractShardId) = {

    val counters = AdaptiveStrategy.CountersExtension(system)(typeName)
    val absoluteAddress = AbsoluteAddress(system)
    val adaptiveExtractShardId = AdaptiveStrategy.extractShardId(counters, extractShardId, msgWeight)
    val adaptiveStrategy = AdaptiveStrategy(
      rebalanceThresholdPercent = rebalanceThresholdPercent,
      counters = counters,
      toAddress = region => absoluteAddress(region.path.address))
    (adaptiveStrategy, adaptiveExtractShardId)
  }
}