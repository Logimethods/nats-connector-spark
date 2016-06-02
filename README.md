# NATS / Spark Connectors

That library provides an [Apache Spark](http://spark.apache.org/) (a fast and general engine for large-scale data processing) integration with the [NATS messaging system](https://nats.io) (a highly performant cloud native messaging system).

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![wercker status](https://app.wercker.com/status/7e12f3a04420e0ee5ec3151341dbda60/s/master "wercker status")](https://app.wercker.com/project/bykey/7e12f3a04420e0ee5ec3151341dbda60)
[![Javadoc](http://javadoc-badge.appspot.com/com.logimethods/nats-connector-spark.svg?label=javadoc)](http://logimethods.github.io/nats-connector-spark/)


## Installation

### Maven Central

#### Releases

The NATS Spark connectors are currently BETA, without being already fully tested in large applications.

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
      <version>0.1.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```
If you don't already have your pom.xml configured for using Maven snapshots, you'll also need to add the following repository to your pom.xml.

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
### From NATS to Spark (Streaming)
```
import com.logimethods.nats.connector.spark.NatsToSparkConnector;
```
```
final SparkConf sparkConf = new SparkConf().setAppName("My Spark Job").setMaster("local[2]");
final JavaSparkContext sc = new JavaSparkContext(sparkConf);
final JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));
```

#### While listening to NATS on a list of subjects:

```
final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY(), "SubjectA", "SubjectB"));
```

#### While listening to a NATS server defined by properties:

```
final Properties properties = new Properties();
properties.setProperty(NatsToSparkConnector.NATS_SUBJECTS, "SubjectA,SubjectB , SubjectC");
final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(properties, StorageLevel.MEMORY_ONLY()));
```

### From Spark (Streaming) to NATS
```
import com.logimethods.nats.connector.spark.SparkToNatsConnector;
```
```
final List<String> data = Arrays.asList(new String[] {
		"data_1",
		"data_2",
		"data_3",
});
JavaRDD<String> rdd = sc.parallelize(data);
```

#### To publish to NATS based on a list of subjects:

```
rdd.foreach(SparkToNatsConnector.publishToNats("SubjectA", "SubjectB"));		
```

#### To publish to a NATS server defined by properties:

```
final Properties properties = new Properties();
properties.setProperty(SparkToNatsConnector.NATS_SUBJECTS, "SubjectA,SubjectB , SubjectC");
rdd.foreach(SparkToNatsConnector.publishToNats(properties));		
```
## Usage (in Scala)
_See the Java code to get the list of the available options (properties, subjects, etc.)._
### From NATS to Spark (Streaming)
```
val messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(properties, StorageLevel.MEMORY_ONLY, inputSubject))
```

### From Spark (Streaming) to NATS
```
val publishToNats = SparkToNatsConnector.publishToNats(properties, outputSubject)
sparkStream.foreachRDD { rdd => rdd.foreach { m => publishToNats.call(m.toString()) }}
```
  
## Testing

JUnit tests are included. To perform those tests, [gnatsd](http://nats.io/download/nats-io/gnatsd/) is required.
Take note that they cannot be run on Eclipse (due to the required NATS server), but with Maven:

```
nats-connector-spark> mvn compile test
```

## Build & Dependencies

- The NATS/Spark Connector library is coded in Java & packaged thanks to Maven as a Jar File.
- That library uses [JNATS](https://github.com/nats-io/jnats) version 0.3.1 to allow compatibility with JVM 1.7 (which is by default used by Spark).
- *The Spark Core & Streaming libraries need to be provided*.

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
