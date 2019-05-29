# Sharding strategies [![Build Status](https://travis-ci.org/evolution-gaming/sharding-strategy.svg)](https://travis-ci.org/evolution-gaming/sharding-strategy) [![Coverage Status](https://coveralls.io/repos/evolution-gaming/sharding-strategy/badge.svg)](https://coveralls.io/r/evolution-gaming/sharding-strategy) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/d60baa5803c542d6b4437cb2a5541ab4)](https://www.codacy.com/app/evolution-gaming/sharding-strategy?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/sharding-strategy&amp;utm_campaign=Badge_Grade) [ ![version](https://api.bintray.com/packages/evolutiongaming/maven/sharding-strategy/images/download.svg) ](https://bintray.com/evolutiongaming/maven/sharding-strategy/_latestVersion) [![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

Alternative to [akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy](https://github.com/akka/akka/blob/master/akka-cluster-sharding/src/main/scala/akka/cluster/sharding/ShardCoordinator.scala#L72)

```scala
trait ShardingStrategy[F[_]] {

  def allocate(requester: Region, shard: Shard, current: Allocation): F[Option[Region]]

  def rebalance(current: Allocation, inProgress: Set[Shard]): F[List[Shard]]
}
```

## Setup

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies += "com.evolutiongaming" %% "sharding-strategy" % "1.0.2"
```
