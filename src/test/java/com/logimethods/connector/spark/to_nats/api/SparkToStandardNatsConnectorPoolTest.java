/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats.api;

import static com.logimethods.connector.nats.spark.UnitTestUtilities.NATS_SERVER_URL;
import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static io.nats.client.Constants.PROP_URL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.logimethods.connector.nats.spark.StandardNatsSubscriber;
import com.logimethods.connector.nats.spark.TestClient;
import com.logimethods.connector.nats.spark.UnitTestUtilities;
import com.logimethods.connector.spark.to_nats.AbstractSparkToNatsConnector;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnector;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnectorPool;
import com.logimethods.connector.spark.to_nats.SparkToStandardNatsConnectorImpl;

//@Ignore
@SuppressWarnings("serial")
public class SparkToStandardNatsConnectorPoolTest implements Serializable {

	protected static final String DEFAULT_SUBJECT = "spark2natsSubject";
//	public static final String NATS_URL = "nats://localhost:4222";
	static JavaStreamingContext ssc;
	static Logger logger = null;
	File tempDir;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		AbstractSparkToNatsConnector.recordConnections = true;

		// Enable tracing for debugging as necessary.
		Level level = Level.WARN;
		UnitTestUtilities.setLogLevel(SparkToNatsConnectorPool.class, level);
		UnitTestUtilities.setLogLevel(SparkToNatsConnector.class, level);
		UnitTestUtilities.setLogLevel(SparkToStandardNatsConnectorImpl.class, level);
		UnitTestUtilities.setLogLevel(SparkToNatsConnectorPool.class, level);
		UnitTestUtilities.setLogLevel(SparkToStandardNatsConnectorPoolTest.class, level);
		UnitTestUtilities.setLogLevel(TestClient.class, level);
		UnitTestUtilities.setLogLevel("org.apache.spark", level);
		UnitTestUtilities.setLogLevel("org.spark-project", level);

		logger = LoggerFactory.getLogger(SparkToStandardNatsConnectorPoolTest.class);       
		
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
		SparkToNatsConnector.CONNECTIONS.clear();

		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("My Spark Streaming Job");
		ssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
	    tempDir = Files.createTempDir();
	    tempDir.deleteOnExit();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	    ssc.stop();
	    ssc = null;
	}


	/**
	 * @return
	 */
	protected List<String> getData() {
		final List<String> data = Arrays.asList(new String[] {
				"data_1",
				"data_2",
				"data_3",
				"data_4",
				"data_5",
				"data_6"
		});
		return data;
	}

	/**
	 * @param data
	 * @return
	 */
	protected StandardNatsSubscriber getStandardNatsSubscriber(final List<String> data, String subject) {
		ExecutorService executor = Executors.newFixedThreadPool(1);

		final StandardNatsSubscriber ns = new StandardNatsSubscriber(NATS_SERVER_URL, subject + "_id", subject, data.size());

		// start the subscribers apps
		executor.execute(ns);

		// wait for subscribers to be ready.
		ns.waitUntilReady();
		return ns;
	}

	@Test(timeout=8000)
	public void testStaticSparkToNatsIncludingMultipleSubjects() throws Exception {   
		final List<String> data = getData();

		final String subject1 = "subject1";
		final StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, subject1);

		final String subject2 = "subject2";
		final StandardNatsSubscriber ns2 = getStandardNatsSubscriber(data, subject2);

		final JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

		SparkToNatsConnectorPool.newPool().withSubjects(DEFAULT_SUBJECT, subject1, subject2).withNatsURL(NATS_SERVER_URL).publishToNats(lines);
		
		ssc.start();

		Thread.sleep(1000);

		final File tmpFile = new File(tempDir.getAbsolutePath(), "tmp.txt");
		final PrintWriter writer = new PrintWriter(tmpFile, "UTF-8");
		for(String str: data) {
			writer.println(str);
		}		
		writer.close();

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}

	@Test(timeout=8000)
	public void testStaticSparkToNatsWithConnectionTimeout() throws Exception {   
		final List<String> data = getData();

		final String subject1 = "subject1";
		final StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, subject1);

		final String subject2 = "subject2";
		final StandardNatsSubscriber ns2 = getStandardNatsSubscriber(data, subject2);

		final JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());
		
		assertTrue("NO connections should be opened when entering the test", SparkToNatsConnector.CONNECTIONS.isEmpty());

		SparkToNatsConnectorPool.newPool()
			.withSubjects(DEFAULT_SUBJECT, subject1, subject2).withNatsURL(NATS_SERVER_URL)
			.withConnectionTimeout(Duration.ofSeconds(2))
			.publishToNats(lines);
		
		ssc.start();

		Thread.sleep(1000);

		final File tmpFile = new File(tempDir.getAbsolutePath(), "tmp.txt");
		final PrintWriter writer = new PrintWriter(tmpFile, "UTF-8");
		for(String str: data) {
			writer.println(str);
		}		
		writer.close();

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
		
		assertFalse("Some connections should have been opened", SparkToNatsConnector.CONNECTIONS.isEmpty());
		
		TimeUnit.SECONDS.sleep(5);
		
		assertTrue("NO connections should be still opened when exiting the test", SparkToNatsConnector.CONNECTIONS.isEmpty());
	}

	@Test(timeout=8000)
	public void testStaticSparkToNatsWithMultipleSubjects() throws Exception {   
		final List<String> data = getData();

		final String subject1 = "subject1";
		final StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, subject1);

		final String subject2 = "subject2";
		final StandardNatsSubscriber ns2 = getStandardNatsSubscriber(data, subject2);

		final JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

		SparkToNatsConnectorPool.newPool().withSubjects(DEFAULT_SUBJECT, subject1, subject2).withNatsURL(NATS_SERVER_URL).publishToNats(lines);
		
		ssc.start();

		Thread.sleep(1000);

		final File tmpFile = new File(tempDir.getAbsolutePath(), "tmp.txt");
		final PrintWriter writer = new PrintWriter(tmpFile, "UTF-8");
		for(String str: data) {
			writer.println(str);
		}		
		writer.close();

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}

	@Test(timeout=8000)
	public void testStaticSparkToNatsWithMultipleProperties() throws Exception {   
		final List<String> data = getData();

		final String subject1 = "subject1";
		final StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, subject1);

		final String subject2 = "subject2";
		final StandardNatsSubscriber ns2 = getStandardNatsSubscriber(data, subject2);

		final JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

		final Properties properties = new Properties();
		properties.setProperty(PROP_URL, NATS_SERVER_URL);
		properties.setProperty(PROP_SUBJECTS, subject1+","+DEFAULT_SUBJECT+" , "+subject2);

		SparkToNatsConnectorPool.newPool().withProperties(properties).publishToNats(lines);
		
		ssc.start();

		Thread.sleep(1000);

		final File tmpFile = new File(tempDir.getAbsolutePath(), "tmp.txt");
		final PrintWriter writer = new PrintWriter(tmpFile, "UTF-8");
		for(String str: data) {
			writer.println(str);
		}		
		writer.close();

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}
}
