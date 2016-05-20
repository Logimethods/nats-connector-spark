# NATS / Spark Connectors

That library provides an [Apache Spark](http://spark.apache.org/) (a fast and general engine for large-scale data processing) integration with the [NATS messaging system](https://nats.io) (a highly performant cloud native messaging system).

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)

## Usage (in Java)
### From NATS to Spark (Streaming)
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

## Testing

JUnit tests are included. To perform those tests, [gnatsd](http://nats.io/download/nats-io/gnatsd/) is required.
Take note that they cannot be run on Eclipse (due to the required NATS server), but with Maven:

```
nats-connector-spark> mvn compile test
```

## Warning
Those connectors are still in BETA version, without being already tested in large applications.

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