# (Java based) NATS / Spark Connectors

That library provides an [Apache Spark](http://spark.apache.org/) (a fast and general engine for large-scale data processing) integration with the [NATS messaging system](https://nats.io) (a highly performant cloud native messaging system) as well as [NATS Streaming](http://www.nats.io/documentation/streaming/nats-streaming-intro/) (a data streaming system powered by NATS).

[![MIT License](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![Issues](https://img.shields.io/github/issues/Logimethods/nats-connector-spark.svg)](https://github.com/Logimethods/nats-connector-spark/issues)
[![wercker status](https://app.wercker.com/status/7e12f3a04420e0ee5ec3151341dbda60/s/master "wercker status")](https://app.wercker.com/project/bykey/7e12f3a04420e0ee5ec3151341dbda60)
[![Javadoc](http://javadoc-badge.appspot.com/com.logimethods/nats-connector-spark.svg?label=javadoc)](http://logimethods.github.io/nats-connector-spark/)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.logimethods/nats-connector-spark/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.logimethods/nats-connector-spark)

## Release Notes
### Version 0.3.0-SNAPSHOT
- Based on Spark 2.0.1
- Spark records can be handled as Key/Value 
- `.asStreamOf(ssc)` is introduced
- Message Data can be any Java `Object` (not limited to `String`), serialized as `byte[]` (the native NATS payload format)

### Version 0.2.0
- A wrapper of that library [dedicated to Scala](https://github.com/Logimethods/nats-connector-spark-scala) has been introduced.
- Introduces connectors to [Nats Streaming](http://www.nats.io/documentation/streaming/nats-streaming-intro/).
- That library uses [JNATS](https://github.com/nats-io/jnats) version 0.4.1, which requires a JVM 1.8.
- That library has a dependence to [NATS Streaming Java Client](https://github.com/nats-io/java-nats-streaming).
- The existing API has been unified (no more `new Object(..., ...).getConnector(..)` like methods, but `Class.newConnector(...).withUrl(...).withSubjects(...)` like ones). That way, the API is less prone to confusion between (optional) parameters.
- To be able to use that connector on a docker-compose based Spark version 1.6.2 Cluster, containers need to belong to an external network (which enforce a hostname without underscore). See [Switch to using hyphens as a separator in hostnames](https://github.com/docker/compose/issues/229):
```Shell
$ docker network create spark
```
Add to your `docker-compose.yml` file the following network:
```YAML
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

The two first versions (0.1.0 & 0.2.0) of the NATS Spark connectors have been released, but without being already fully tested in large applications.

If you are embedding the NATS Spark connectors, add the following dependency to your project's `pom.xml`.

```xml
  <dependencies>
    ...
    <dependency>
      <groupId>com.logimethods</groupId>
      <artifactId>nats-connector-spark</artifactId>
      <version>0.2.0</version>
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
      <version>0.3.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```
If you don't already have your `pom.xml` configured for using Maven snapshots from Sonatype / Nexus, you'll also need to add the following repository to your `pom.xml`.

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

The reception of NATS Messages as Spark Steam is done through the `NatsToSparkConnector.receiveFromNats([Class], ...)` method, where `[Class]` is the Java Class of the objects to deserialize:

```java
JavaReceiverInputDStream<[Class]> messages = 
	NatsToSparkConnector
		.receiveFromNats([Class].class, StorageLevel.MEMORY_ONLY()
		.../...
		.asStreamOf(ssc);
```

#### Deserialization of the primitive types

Those objects need first to be serialized as `byte[]` using the right protocol before being push into the NATS messages payload.
By default, the primitive Java types are decoded through the following method of  `com.logimethods.connector.nats_spark.NatsSparkUtilities`:

```java
public static <X> X decodeData(Class<X> type, byte[] bytes) throws UnsupportedOperationException {
	if (type == String.class) {
		return (X) new String(bytes);
	}
	if ((type == Double.class) || (type == double.class)){
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return (X) new Double(buffer.getDouble());
	}
	if ((type == Float.class) || (type == float.class)){
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return (X) new Float(buffer.getFloat());
	}
	if ((type == Integer.class) || (type == int.class)){
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return (X) new Integer(buffer.getInt());
	}
	if ((type == Long.class) || (type == long.class)){
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return (X) new Long(buffer.getLong());
	}
	if ((type == Byte.class) || (type == byte.class)){
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return (X) new Byte(buffer.get());
	}
	if ((type == Character.class) || (type == char.class)){
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return (X) new Character(buffer.getChar());
	}
	if ((type == Short.class) || (type == short.class)){
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return (X) new Short(buffer.getShort());
	}
	throw new UnsupportedOperationException("It is not possible to extract Data of type " + type);
}
```
Therefore, you can use the opposite method to encode the Data:

```java
public static byte[] encodeData(Object obj) {
	if (obj instanceof String) {
		return ((String) obj).getBytes();
	}
	if (obj instanceof Double) {
		return ByteBuffer.allocate(Double.BYTES).putDouble((Double) obj).array();
	}
	if (obj instanceof Float) {
		return ByteBuffer.allocate(Float.BYTES).putFloat((Float) obj).array();
	}
	if (obj instanceof Integer) {
		return ByteBuffer.allocate(Integer.BYTES).putInt((Integer) obj).array();
	}
	if (obj instanceof Long) {
		return ByteBuffer.allocate(Long.BYTES).putLong((Long) obj).array();
	}
	if (obj instanceof Byte) {
		return ByteBuffer.allocate(Byte.BYTES).put((Byte) obj).array();
	}
	if (obj instanceof Character) {
		return ByteBuffer.allocate(Character.BYTES).putChar((Character) obj).array();
	}
	if (obj instanceof Short) {
		return ByteBuffer.allocate(Short.BYTES).putShort((Short) obj).array();
	}
	throw new UnsupportedOperationException("It is not possible to encode Data of type " + obj.getClass());
}
```

#### Custom Deserialization

For more complex types, you should provide your own decoder through the `withDataDecoder(Function<byte[], V> dataDecoder)` method:

```java
final Function<byte[], MyClass> dataDecoder = bytes -> {
	.../...
	return (MyClass) obj;
};
JavaReceiverInputDStream<MyClass> messages = 
	NatsToSparkConnector
		.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY()
		.../...
		.withDataDecoder(dataDecoder)
		.asStreamOf(ssc);
```

#### From NATS to Spark (Streaming)
```java
import com.logimethods.nats.connector.spark.NatsToSparkConnector;
```
```java
SparkConf sparkConf = new SparkConf().setAppName("My Spark Job").setMaster("local[2]");
JavaSparkContext sc = new JavaSparkContext(sparkConf);
JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));
```

##### While listening to NATS on a list of subjects:

```java
JavaReceiverInputDStream<String> messages = 
	NatsToSparkConnector
		.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY()
		.withSubjects("SubjectA", "SubjectB")
		.withNatsURL("nats://localhost:4222")
		.asStreamOf(ssc);
```

##### While listening to a NATS server defined by properties:

```java
Properties properties = new Properties();
properties.setProperty(com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS, "SubjectA,SubjectB , SubjectC");
JavaReceiverInputDStream<Float> messages = 
	NatsToSparkConnector
		.receiveFromNats(Float.class, StorageLevel.MEMORY_ONLY())
		.withProperties(properties)
		.asStreamOf(ssc);
```

The optional settings are:

* `withSubjects(String... subjects)`
* `withNatsURL(String natsURL)`
* `withProperties(Properties properties)`

#### From NATS to Spark (Streaming) stored as *Key / Values*

The Spark Stream is there made of [Key/Value Pairs](https://spark.apache.org/docs/2.0.1/api/java/org/apache/spark/streaming/api/java/JavaPairDStream.html), where the Key is the _Subject_ and the Value is the _Payload_ of the NATS Messages.

```
JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

JavaPairDStream<String, Integer> messages = 
	NatsToSparkConnector
		.receiveFromNats(Integer.class, StorageLevel.MEMORY_ONLY()
		.withSubjects("SubjectA.>", "SubjectB.*.result")
		.withNatsURL("nats://localhost:4222")
		.asStreamOfKeyValue(ssc);
				
messages.groupByKey().print();
```

#### From *NATS Streaming* to Spark (Streaming)

```java
String clusterID = "test-cluster";
Instant start = Instant.now().minus(30, ChronoUnit.MINUTES);
JavaReceiverInputDStream<String> messages = 
	NatsToSparkConnector
		.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), clusterID)
		.withNatsURL(STAN_URL)
		.withSubjects(DEFAULT_SUBJECT)
		.setDurableName("MY_DURABLE_NAME")
		.startAtTime(start)
		.asStreamOf(ssc);
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

#### From *NATS Streaming* to Spark (Streaming) stored as *Key / Values*

The Spark Stream is there made of [Key/Value Pairs](https://spark.apache.org/docs/2.0.1/api/java/org/apache/spark/streaming/api/java/JavaPairDStream.html), where the Key is the _Subject_ and the Value is the _Payload_ of the NATS Messages.

```
JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

final JavaPairDStream<String, Integer> messages = 
		NatsToSparkConnector
				.receiveFromNatsStreaming(Integer.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
				.withNatsURL(STAN_URL)
				.withSubjects(DEFAULT_SUBJECT)
				.asStreamOfKeyValue(ssc);
				
messages.groupByKey().print();
```

### From Spark to NATS

#### Serialization of the primitive types

The Spark elements are first serialized as `byte[]` before being sent to NATS. By default, the primitive Java types are encoded through the `com.logimethods.connector.nats_spark.NatsSparkUtilities.encodeData(Object obj)` method (see above).

#### Custom Serialization

Custom serialization can be performed by a `java.util.function.Function<[Class],  byte[]> & Serializable)` function provided through the `.publishToNats(...)` method, like:

```java
SparkToNatsConnectorPool.newPool()
			.withNatsURL(NATS_SERVER_URL)
			.publishToNats(stream, 
				       (java.util.function.Function<String, byte[]> & Serializable) str -> str.getBytes());
```

#### From Spark (Streaming) to NATS

```java
import com.logimethods.nats.connector.spark.SparkToNatsConnectorPool;
JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());
```
```java
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

#### From Spark (Streaming) made of *Key/Value* Pairs to NATS

Any Spark Stream of type [JavaPairDStream\<String, String\>](https://spark.apache.org/docs/2.0.1/api/java/org/apache/spark/streaming/api/java/JavaPairDStream.html) will publish NATS Messages where the Subject is a composition of the (optional) _Global Subject(s)_ and the _Key_ of the Pairs ; while the NATS _Payload_ will be the Pair's _Value_.

##### Without Global Subjects

```java
JavaPairDStream<String, String> stream = 
	lines.mapToPair((PairFunction<String, String, String>) str -> {return new Tuple2<String, String>("B", str);});

SparkToNatsConnectorPool
	.newPool()
	.withNatsURL(NATS_SERVER_URL)
	.withConnectionTimeout(Duration.ofSeconds(2))
	.publishToNatsAsKeyValue(stream);
```
will send to NATS such [subject:payload] messages:
```
[B:string1]
[B:string1]
[B:string2]
[B:string2]
...
```

##### With Global Subjects

```java
JavaPairDStream<String, String> stream = 
	lines.mapToPair((PairFunction<String, String, String>) str -> {return new Tuple2<String, String>("B", str);});

SparkToNatsConnectorPool
	.newPool()
	.withNatsURL(NATS_SERVER_URL)
	.withConnectionTimeout(Duration.ofSeconds(2))
	.withSubjects("A1.", "A2.")
	.publishToNatsAsKeyValue(stream);
```
will send to NATS such [subject:payload] messages:
```
[A1.B:string1]
[A2.B:string1]
[A1.B:string2]
[A2.B:string2]
...
```

##### With Global Subjects defined as Regex Replacements

Here the NATS Subjects will be the Key of the Spark Pairs where the pattern expressed by the left part of the Global Subject (split by `=>`) is replaced by the right part of the Global Subject.

```java
JavaPairDStream<String, String> stream = 
	lines.mapToPair((PairFunction<String, String, String>) str -> {return new Tuple2<String, String>("b.c", str);});

SparkToNatsConnectorPool
	.newPool()
	.withNatsURL(NATS_SERVER_URL)
	.withConnectionTimeout(Duration.ofSeconds(2))
	.withSubjects("b.=>A1.", "*.=>A2.")
	.publishToNatsAsKeyValue(stream);
```
will send to NATS such [subject:payload] messages:
```
[A1.c:string1]
[A2.c:string1]
[A1.c:string2]
[A2.c:string2]
...
```

See 
```java
@Test
public void testCombineSubjectsWithSubstitution() {
	assertEquals("A.C", combineSubjects("*. =>A.", "B.C"));
	assertEquals("A.C.D", combineSubjects("*. => A.", "B.C.D"));
	assertEquals("B.C.D", combineSubjects("X.=>A.", "B.C.D"));
	assertEquals("A.D", combineSubjects("*.*.=>A.", "B.C.D"));
	assertEquals("A.B.D", combineSubjects("*.C=>A.B", "B.C.D"));
	assertEquals("B.C.D", combineSubjects("*.X.*=>A.B", "B.C.D"));
	assertEquals("A.b.C.D", combineSubjects("B=>b", "A.B.C.D"));
	assertEquals("A.B.C.D", combineSubjects("^B=>b", "A.B.C.D"));
	assertEquals("A.b.B.D", combineSubjects("B=>b", "A.B.B.D"));
}
```

#### From Spark (Streaming) to *NATS Streaming*

```java
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

#### From Spark (Streaming) made of *Key/Value* Pairs to *NATS Streaming*

```java
JavaPairDStream<String, String> stream = 
	lines.mapToPair((PairFunction<String, String, String>) str -> {return new Tuple2<String, String>("B", str);});
String clusterID = "test-cluster";
SparkToNatsConnectorPool
	.newStreamingPool(clusterID)
	.withConnectionTimeout(Duration.ofSeconds(6))
	.withSubjects("subject1", "subject2")
	.withNatsURL(STAN_URL)
	.publishToNatsAsKeyValue(stream);
```

#### From Spark (*WITHOUT Streaming NOR Spark Cluster*) to NATS
```java
import com.logimethods.nats.connector.spark.SparkToNatsConnector;

List<String> data = getData();
JavaRDD<String> rdd = sc.parallelize(data);
```
```java
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

#### From Spark (*WITHOUT Streaming NOR Spark Cluster*) made of *Key/Value* Pairs to NATS

A Spark `JavaRDD<Tuple2<String, String>>` can publish NATS Messages where the Subject is a composition of the (optional) _Global Subject(s)_ and the _First Element_ of the Pairs ; while the NATS _Payload_ will be the Pair's _Second Element_.

To do so, you should use `.publishAsKeyValueToNats()` instead of `.publishToNats()`.

```java
JavaRDD<Tuple2<String, Integer>> tuples = 
	rdd.map((Function<String, Tuple2<String, Integer>>) 
			str -> {return new Tuple2<String, Integer>("sub-subject", Integer.parseInt(str));});	
	
final VoidFunction<Tuple2<String, Integer>> publishToNats = 
		SparkToNatsConnector
			.newConnection()
			.withNatsURL(NATS_SERVER_URL)
			.withSubjects("main-subject.")
			.publishToNatsAsKeyValue();

tuples.foreach(publishToNats);	
```

## Usage (in Scala)
You should instead use the dedicated [nats-connector-spark-scala](https://github.com/Logimethods/nats-connector-spark-scala) connector.

## Testing

JUnit tests are included. To perform those tests, [gnatsd](http://nats.io/download/nats-io/gnatsd/) and [nats-streaming-server](http://nats.io/documentation/streaming/nats-streaming-intro/) are required.
You might have first to start those servers:
```Shell
gnatsd -p 4221&
nats-streaming-server -p 4223&
````
Then call Maven:
```Shell
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
