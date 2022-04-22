## Versions
From version `2.0.0` this library is based on **cats-effect-3**. If you want to use it with **cats-effect-2**,
you need to use version prior to `2.0.0`, for example, `1.0.7`

# Sharding Strategies
[![Build Status](https://github.com/evolution-gaming/sharding-strategy/workflows/CI/badge.svg)](https://github.com/evolution-gaming/sharding-strategy/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/evolution-gaming/sharding-strategy/badge.svg?branch=master)](https://coveralls.io/github/evolution-gaming/sharding-strategy?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/d60baa5803c542d6b4437cb2a5541ab4)](https://www.codacy.com/app/evolution-gaming/sharding-strategy?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/sharding-strategy&amp;utm_campaign=Badge_Grade)
[![Version](https://img.shields.io/badge/version-click-blue)](https://evolution.jfrog.io/artifactory/api/search/latestVersion?g=com.evolutiongaming&a=sharding-strategy_2.13&repos=public)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

Alternative to [akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy](https://github.com/akka/akka/blob/master/akka-cluster-sharding/src/main/scala/akka/cluster/sharding/ShardCoordinator.scala#L72)

## Api

```scala
trait ShardingStrategy[F[_]] {

  def allocate(requester: Region, shard: Shard, current: Allocation): F[Option[Region]]

  def rebalance(current: Allocation, inProgress: Set[Shard]): F[List[Shard]]
}
```

## Syntax

```scala
val strategy = LeastShardsStrategy()
  .filterShards(...)
  .filterRegions(...)
  .rebalanceThreshold(10)
  .takeShards(10) 
  .shardRebalanceCooldown(1.minute)
  .logging(...)
  .toAllocationStrategy()
```

## Setup
For **cats-effect-3**
```scala
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

libraryDependencies += "com.evolutiongaming" %% "sharding-strategy" % "2.0.0"
```
For **cats-effect-2**
```scala
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

libraryDependencies += "com.evolutiongaming" %% "sharding-strategy" % "1.0.7"
```