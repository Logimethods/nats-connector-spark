/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NatsToSparkConnectorTest {

	protected static String DEFAULT_SUBJECT_ROOT = "nats2sparkSubject";
	protected static int DEFAULT_SUBJECT_INR = 0;
	protected static String DEFAULT_SUBJECT;
	protected static JavaSparkContext sc;
	protected static AtomicInteger TOTAL_COUNT = new AtomicInteger();;
	static Logger logger = null;
	static Boolean rightNumber = true;
	static Boolean atLeastSomeData = false;
	static String payload = null;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Enable tracing for debugging as necessary.
		System.setProperty("org.slf4j.simpleLogger.log.com.logimethods.nats.connector.spark.NatsToSparkConnector", "trace");
		System.setProperty("org.slf4j.simpleLogger.log.com.logimethods.nats.connector.spark.NatsToSparkConnectorTest", "debug");
		System.setProperty("org.slf4j.simpleLogger.log.com.logimethods.nats.connector.spark.TestClient", "trace");

		logger = LoggerFactory.getLogger(NatsToSparkConnectorTest.class);       

		UnitTestUtilities.startDefaultServer();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		UnitTestUtilities.stopDefaultServer();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		assertTrue(logger.isDebugEnabled());
		assertTrue(LoggerFactory.getLogger(NatsToSparkConnector.class).isTraceEnabled());
		
		DEFAULT_SUBJECT = DEFAULT_SUBJECT_ROOT + (DEFAULT_SUBJECT_INR++);
		TOTAL_COUNT.set(0);
		
		rightNumber = true;
		atLeastSomeData = false;
		
		SparkConf sparkConf = new SparkConf().setAppName("My Spark Job").setMaster("local[2]");
		sc = new JavaSparkContext(sparkConf);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		sc.stop();
	}
	
	/**
	 * Test method for {@link com.logimethods.nats.connector.spark.NatsToSparkConnector#receiveFromNats(java.lang.String, int, java.lang.String, java.lang.String)}.
	 * @throws InterruptedException 
	 */
	@Test
	public void testNatsToSparkConnectorWithPropertiesAndSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(properties, StorageLevel.MEMORY_ONLY(), DEFAULT_SUBJECT));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test
	public void testNatsToSparkConnectorWithSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY(), DEFAULT_SUBJECT));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test
	public void testNatsToSparkConnectorWithPropertiesAndMultipleSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(properties, StorageLevel.MEMORY_ONLY(), DEFAULT_SUBJECT, "EXTRA_SUBJECT"));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	/**
	 * Test method for {@link com.logimethods.nats.connector.spark.NatsToSparkConnector#receiveFromNats(java.lang.String, int, java.lang.String)}.
	 * @throws InterruptedException 
	 */
	@Test
	public void testNatsToSparkConnectorWithProperties() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		properties.setProperty(NatsToSparkConnector.NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(properties, StorageLevel.MEMORY_ONLY()));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	/**
	 * Test method for {@link com.logimethods.nats.connector.spark.NatsToSparkConnector#receiveFromNats(java.lang.String, int, java.lang.String)}.
	 * @throws Exception 
	 */
	@Test
	public void testNatsToSparkConnectorWITHOUTProperties() throws Exception {
		
		try {
			NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).receive();
		} catch (Exception e) {
			if (e.getMessage().contains("NatsToSparkConnector needs at least one NATS Subject"))
				return;
			else
				throw e;
		}	

		fail("An Exception(\"NatsToSparkConnector needs at least one Subject\") should have been raised.");
	}
	
	@Test
	public void testNatsToSparkConnectorWithSystemProperties() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		System.setProperty(NatsToSparkConnector.NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");

		try {
			final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()));

			validateTheReceptionOfMessages(ssc, messages);
		} finally {
			System.clearProperty(NatsToSparkConnector.NATS_SUBJECTS);
		}		
	}
	

	private void validateTheReceptionOfMessages(JavaStreamingContext ssc,
			JavaReceiverInputDStream<String> stream) throws InterruptedException {
		JavaDStream<String> messages = stream.repartition(3);

		ExecutorService executor = Executors.newFixedThreadPool(6);

		final int nbOfMessages = 5;
		NatsPublisher np = new NatsPublisher("np", DEFAULT_SUBJECT,  nbOfMessages);
		
		messages.print();
		
		messages.foreachRDD(new Function<JavaRDD<String>, Void>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
				logger.debug("RDD received: {}", rdd.collect());
				
				final long count = rdd.count();
				if ((count != 0) && (count != nbOfMessages)) {
					rightNumber = false;
					logger.error("The number of messages received should have been {} instead of {}.", nbOfMessages, count);
				}
				
				TOTAL_COUNT.getAndAdd((int) count);
				
				atLeastSomeData = atLeastSomeData || (count > 0);
				
				for (String str :rdd.collect()) {
					if (! str.startsWith(NatsPublisher.NATS_PAYLOAD)) {
							payload = str;
						}
				}
				
				return null;
			}			
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
