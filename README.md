# (Java based) NATS / Spark Connectors

That library provides an [Apache Spark](http://spark.apache.org/) (a fast and general engine for large-scale data processing) integration with the [NATS messaging system](https://nats.io) (a highly performant cloud native messaging system) as well as [NATS Streaming](http://www.nats.io/documentation/streaming/nats-streaming-intro/) (a data streaming system powered by NATS).

[![MIT License](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![Issues](https://img.shields.io/github/issues/Logimethods/nats-connector-spark.svg)](https://github.com/Logimethods/nats-connector-spark/issues)
[![wercker status](https://app.wercker.com/status/7e12f3a04420e0ee5ec3151341dbda60/s/master "wercker status")](https://app.wercker.com/project/bykey/7e12f3a04420e0ee5ec3151341dbda60)
[![Javadoc](http://javadoc-badge.appspot.com/com.logimethods/nats-connector-spark.svg?label=javadoc)](http://logimethods.github.io/nats-connector-spark/)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.logimethods/nats-connector-spark/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.logimethods/nats-connector-spark)

## Release Notes
### Version 0.2.0-SNAPSHOT
- Introduces connectors to [Nats Streaming](http://www.nats.io/documentation/streaming/nats-streaming-intro/).
- That library uses [JNATS](https://github.com/nats-io/jnats) version 0.4.1, which requires a JVM 1.8.
- The existing API has been unified (no more `new Object(..., ...).getConnector(..)` like methods, but `Class.newConnector(...).withUrl(...).withSubjects(...)` like ones). That way, the API is less prone to confusion between (optional) parameters.
- To be able to use that connector on a docker-compose based Spark version 1.6.2 Cluster, containers need to belong to an external network (which enforce a hostname without underscore). See [Switch to using hyphens as a separator in hostnames](https://github.com/docker/compose/issues/229):
```
$ docker network create spark
```
Add to your `docker-compose.yml` file the following network:
```
networks:
  default:
    external:
      name: spark
```

### Version 0.1.0
- That library uses [JNATS](https://github.com/nats-io/jnats) version 0.3.1 to allow compatibility with JVM 1.7 (which is by default used by Spark).
- Is based on Spark version 1.5.2 to be able to use docker-compose without hostname constrains. See [Underscore in domain names](https://forums.docker.com/t/underscore-in-domain-names/12584/2).
- **For an accurate documentation regarding that version, please follow the [0.1.0 branch](https://github.com/Logimethods/nats-connector-spark/tree/version_0.1.0)**.

## Installation

### Maven Central

#### Releases

The first version (0.1.0) of the NATS Spark connectors has been released, but without being already fully tested in large applications.

If you are embedding the NATS Spark connectors, add the following dependency to your project's `pom.xml`.

```xml
  <dependencies>
    ...
    <dependency>
      <groupId>com.logimethods</groupId>
      <artifactId>nats-connector-spark</artifactId>
      <version>0.1.0</version>
    </dependency>
  </dependencies>
```
If you don't already have your pom.xml configured for using Maven releases from Sonatype / Nexus, you'll also need to add the following repository to your pom.xml.

```xml
<repositories>
    ...
    <repository>
        <id>sonatype-oss-public</id>
        <url>https://oss.sonatype.org/content/groups/public/</url>
        <releases>
            <enabled>true</enabled>
        </releases>
    </repository>
</repositories>
```
#### Snapshots

Snapshots are regularly uploaded to the Sonatype OSSRH (OSS Repository Hosting) using
the same Maven coordinates.
If you are embedding the NATS Spark connectors, add the following dependency to your project's `pom.xml`.

```xml
  <dependencies>
    ...
    <dependency>
      <groupId>com.logimethods</groupId>
      <artifactId>nats-connector-spark</artifactId>
      <version>0.2.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```
If you don't already have your pom.xml configured for using Maven snapshots from Sonatype / Nexus, you'll also need to add the following repository to your pom.xml.

```xml
<repositories>
    ...
    <repository>
        <id>sonatype-snapshots</id>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

## Usage (in Java)
### From NATS to Spark
#### From NATS to Spark (Streaming)
```
import com.logimethods.nats.connector.spark.NatsToSparkConnector;
```
```
SparkConf sparkConf = new SparkConf().setAppName("My Spark Job").setMaster("local[2]");
JavaSparkContext sc = new JavaSparkContext(sparkConf);
JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));
```

##### While listening to NATS on a list of subjects:

```
JavaReceiverInputDStream<String> messages = 
	ssc.receiverStream(
		NatsToSparkConnector
			.receiveFromNats(StorageLevel.MEMORY_ONLY()
			.withSubjects("SubjectA", "SubjectB")
			.withNatsURL("nats://localhost:4222") );
```

##### While listening to a NATS server defined by properties:

```
Properties properties = new Properties();
properties.setProperty(com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS, "SubjectA,SubjectB , SubjectC");
JavaReceiverInputDStream<String> messages = 
	ssc.receiverStream(
		NatsToSparkConnector
			.receiveFromNats(StorageLevel.MEMORY_ONLY())
			.withProperties(properties) );
```

The optional settings are:
* `withSubjects(String... subjects)`
* `withNatsURL(String natsURL)`
* `withProperties(Properties properties)`

#### From *NATS Streaming* to Spark (Streaming)
```
String clusterID = "test-cluster";
Instant start = Instant.now().minus(30, ChronoUnit.MINUTES);
JavaReceiverInputDStream<String> messages = 
	ssc.receiverStream(
		NatsToSparkConnector
			.receiveFromNatsStreaming(StorageLevel.MEMORY_ONLY(), clusterID)
			.withNatsURL(STAN_URL)
			.withSubjects(DEFAULT_SUBJECT)
			.setDurableName("MY_DURABLE_NAME")
			.startAtTime(start) );
```

The optional settings are:
* `withSubjects(String... subjects)`
* `withNatsURL(String natsURL)`
* `withProperties(Properties properties)`

as well as options related to [NATS Streaming](https://github.com/nats-io/java-nats-streaming):

* `withSubscriptionOptionsBuilder(io.nats.stan.SubscriptionOptions.Builder optsBuilder)`
* `setDurableName(String durableName)`
* `setMaxInFlight(int maxInFlight)`
* `setAckWait(Duration ackWait)`
* `setAckWait(long ackWait, TimeUnit unit)`
* `setManualAcks(boolean manualAcks)`
* `startAtSequence(long seq)`
* `startAtTime(Instant start)`
* `startAtTimeDelta(long ago, TimeUnit unit)`
* `startAtTimeDelta(Duration ago)`
* `startWithLastReceived()`
* `deliverAllAvailable()`

### From Spark to NATS
#### From Spark (Streaming) to NATS
```
import com.logimethods.nats.connector.spark.SparkToNatsConnectorPool;
JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());
```
```
SparkToNatsConnectorPool
	.newPool()
	.withSubjects("subject1", "subject2")
	.withNatsURL(NATS_SERVER_URL)
	.withConnectionTimeout(Duration.ofSeconds(6))
	.publishToNats(lines);
```

The optional settings are:
* `withSubjects(String... subjects)`
* `withNatsURL(String natsURL)`
* `withProperties(Properties properties)`
* `withConnectionTimeout(Duration duration)`

#### From Spark (Streaming) to *NATS Streaming*

```
String clusterID = "test-cluster";
SparkToNatsConnectorPool
	.newStreamingPool(clusterID)
	.withConnectionTimeout(Duration.ofSeconds(6))
	.withSubjects("subject1", "subject2")
	.withNatsURL(STAN_URL)
	.publishToNats(lines);
```

The optional settings are:
* `withSubjects(String... subjects)`
* `withNatsURL(String natsURL)`
* `withProperties(Properties properties)`
* `withConnectionTimeout(Duration duration)`

#### From Spark (*WITHOUT Streaming NOR Spark Cluster*) to NATS
```
import com.logimethods.nats.connector.spark.SparkToNatsConnector;

List<String> data = getData();
JavaRDD<String> rdd = sc.parallelize(data);
```
```
rdd.foreach(
	SparkToNatsConnector
		.newConnection()
		.withNatsURL(NATS_SERVER_URL)
		.withSubjects("subject1", "subject2")
		.withConnectionTimeout(Duration.ofSeconds(1))
		.publishToNats()); 
```

The optional settings are:
* `withSubjects(String... subjects)`
* `withNatsURL(String natsURL)`
* `withProperties(Properties properties)`
* `withConnectionTimeout(Duration duration)`

## Usage (in Scala)
You should instead use the dedicated [nats-connector-spark-scala](https://github.com/Logimethods/nats-connector-spark-scala) connector.

## Testing

JUnit tests are included. To perform those tests, [gnatsd](http://nats.io/download/nats-io/gnatsd/) is required.
Take note that they cannot be run on Eclipse (due to the required NATS server), but with Maven:

```
nats-connector-spark> mvn compile test
```

Those connectors have been tested against a Spark Cluster, thanks to the [Docker Based Application](https://github.com/Logimethods/docker-nats-connector-spark).

## Build & Dependencies

- The NATS/Spark Connector library is coded in Java & packaged thanks to Maven as a Jar File.
- *The Spark Core & Streaming libraries need to be provided*.

## Samples
* The ['docker-nats-connector-spark'](https://github.com/Logimethods/docker-nats-connector-spark) Docker Based Project that makes use of Gatling, Spark & NATS.

## License

(The MIT License)

Copyright (c) 2016 Logimethods.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
