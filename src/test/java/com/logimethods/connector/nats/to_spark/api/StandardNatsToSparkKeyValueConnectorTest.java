/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark.api;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.NATS_SERVER_URL;
import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static io.nats.client.Constants.PROP_URL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import com.logimethods.connector.nats.spark.test.NatsPublisher;
import com.logimethods.connector.nats.spark.test.StandardNatsPublisher;
import com.logimethods.connector.nats.to_spark.NatsToSparkConnector;
import com.logimethods.connector.nats.to_spark.StandardNatsToKeyValueSparkConnectorImpl;

import scala.Tuple2;

public class StandardNatsToSparkKeyValueConnectorTest extends AbstractNatsToSparkTest implements Serializable {
	
	@Override
	protected NatsPublisher getNatsPublisher(final int nbOfMessages) {
		return new StandardNatsPublisher("np", NATS_SERVER_URL, DEFAULT_SUBJECT,  nbOfMessages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalPropertiesAndSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		properties.setProperty(PROP_URL, NATS_SERVER_URL);
		final StandardNatsToKeyValueSparkConnectorImpl connector = 
				NatsToSparkConnector
					.receiveFromNats(StorageLevel.MEMORY_ONLY())
					.storedAsKeyValue()
					.withProperties(properties)
					.withSubjects(DEFAULT_SUBJECT);
		final JavaReceiverInputDStream<Tuple2<String, String>> messages = ssc.receiverStream(connector);

		validateTheReceptionOfKeyValueMessages(ssc, messages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final StandardNatsToKeyValueSparkConnectorImpl connector = 
				NatsToSparkConnector
					.receiveFromNats(StorageLevel.MEMORY_ONLY())
					.withNatsURL(NATS_SERVER_URL)
					.withSubjects(DEFAULT_SUBJECT)
					.storedAsKeyValue();
		final JavaReceiverInputDStream<Tuple2<String, String>> messages =  ssc.receiverStream(connector);

		validateTheReceptionOfKeyValueMessages(ssc, messages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalPropertiesAndMultipleSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		final StandardNatsToKeyValueSparkConnectorImpl connector = 
				NatsToSparkConnector
					.receiveFromNats(StorageLevel.MEMORY_ONLY())
					.withNatsURL(NATS_SERVER_URL)
					.withProperties(properties)
					.withSubjects(DEFAULT_SUBJECT, "EXTRA_SUBJECT")
					.storedAsKeyValue();
		final JavaReceiverInputDStream<Tuple2<String, String>> messages = ssc.receiverStream(connector);

		validateTheReceptionOfKeyValueMessages(ssc, messages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalProperties() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		properties.setProperty(PROP_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		properties.setProperty(PROP_URL, NATS_SERVER_URL);
		final StandardNatsToKeyValueSparkConnectorImpl connector = 
				NatsToSparkConnector
					.receiveFromNats(StorageLevel.MEMORY_ONLY())
					.storedAsKeyValue()
					.withProperties(properties);
		final JavaReceiverInputDStream<Tuple2<String, String>> messages = ssc.receiverStream(connector);

		validateTheReceptionOfKeyValueMessages(ssc, messages);
	}

	protected void validateTheReceptionOfKeyValueMessages(JavaStreamingContext ssc,
			JavaReceiverInputDStream<Tuple2<String, String>> stream) throws InterruptedException {
		JavaPairDStream<String, String> messages = stream.mapToPair(
				new PairFunction<Tuple2<String, String>, String, String>() {
					private static final long serialVersionUID = 7263395690866443489L;

					@Override
					public Tuple2<String, String> call(Tuple2<String,String> tuple) {
						return tuple;
					}
			});

		ExecutorService executor = Executors.newFixedThreadPool(6);

		final int nbOfMessages = 5;
		NatsPublisher np = getNatsPublisher(nbOfMessages);
		
		if (logger.isDebugEnabled()) {
			messages.print();
		}
		
		JavaPairDStream<String, Integer> pairs = messages.mapToPair(s -> new Tuple2(s._1, 1));		
		JavaPairDStream<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
		counts.print();
		
		counts.foreachRDD((VoidFunction<JavaPairRDD<String, Integer>>) pairRDD -> {
			pairRDD.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
				final long count = tuple._2;
				if ((count != 0) && (count != nbOfMessages)) {
					rightNumber = false;
					logger.error("The number of messages received should have been {} instead of {}.", nbOfMessages, count);
				}

				TOTAL_COUNT.getAndAdd((int) count);

				atLeastSomeData = atLeastSomeData || (count > 0);
			});
		});
		
		ssc.start();		
		Thread.sleep(1000);		
		// start the publisher
		executor.execute(np);
		np.waitUntilReady();		
		Thread.sleep(500);
		ssc.close();		
		Thread.sleep(500);
		assertTrue("Not a single RDD did received messages.", atLeastSomeData);	
		assertTrue("Not the right number of messages have been received", rightNumber);
		assertEquals(nbOfMessages, TOTAL_COUNT.get());
		assertNull("'" + payload + " should be '" + NatsPublisher.NATS_PAYLOAD + "'", payload);		
	}
}
