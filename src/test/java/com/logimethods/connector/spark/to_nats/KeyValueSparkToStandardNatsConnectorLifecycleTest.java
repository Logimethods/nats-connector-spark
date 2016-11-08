/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.NATS_SERVER_URL;

import java.time.Duration;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

//@Ignore
@SuppressWarnings("serial")
public class KeyValueSparkToStandardNatsConnectorLifecycleTest extends AbstractSparkToStandardNatsConnectorLifecycleTest {

	protected void publishToNats(final String subject1, final String subject2, final int partitionsNb) {
		final JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath()).repartition(partitionsNb);		
		
		JavaPairDStream<String, String> stream1 = 
				lines.mapToPair((PairFunction<String, String, String>) str -> {
									return new Tuple2<String, String>(subject1, str);
								});
		JavaPairDStream<String, String> stream2 = 
				lines.mapToPair((PairFunction<String, String, String>) str -> {
									return new Tuple2<String, String>(subject2, str);
								});
		final JavaPairDStream<String, String> stream = stream1.union(stream2);
		
		if (logger.isDebugEnabled()) {
			stream.print();
		}		
		
		SparkToNatsConnectorPool
			.newPool()
			.withNatsURL(NATS_SERVER_URL)
			.withConnectionTimeout(Duration.ofSeconds(2))
			.publishToNatsAsKeyValue(stream);
	}
}
