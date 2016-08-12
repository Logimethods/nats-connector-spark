/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats.api;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
//import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats.spark.StandardNatsSubscriber;
import com.logimethods.connector.nats.spark.TestClient;
import com.logimethods.connector.nats.spark.UnitTestUtilities;
import com.logimethods.connector.spark.to_nats.AbstractSparkToNatsConnector;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnector;
import com.logimethods.connector.spark.to_nats.SparkToStandardNatsConnectorImpl;

import static com.logimethods.connector.nats.spark.UnitTestUtilities.NATS_SERVER_URL;
import static com.logimethods.connector.nats_spark.Constants.*;
import static io.nats.client.Constants.*;
import static org.junit.Assert.*;

//@Ignore
public class SparkToStandardNatsConnectorTest {

	protected static final String DEFAULT_SUBJECT = "spark2natsSubject";
	protected static JavaSparkContext sc;
	static Logger logger = null;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Enable tracing for debugging as necessary.
		Level level = Level.TRACE;
		UnitTestUtilities.setLogLevel(SparkToNatsConnector.class, level);
		UnitTestUtilities.setLogLevel(SparkToStandardNatsConnectorImpl.class, level);
		UnitTestUtilities.setLogLevel(SparkToStandardNatsConnectorTest.class, level);
		UnitTestUtilities.setLogLevel(TestClient.class, level);
		UnitTestUtilities.setLogLevel("org.apache.spark", level);
		UnitTestUtilities.setLogLevel("org.spark-project", level);
		
		logger = LoggerFactory.getLogger(SparkToStandardNatsConnectorTest.class);       

		SparkConf sparkConf = new SparkConf().setAppName("My Spark Job").setMaster("local[2]");
		sc = new JavaSparkContext(sparkConf);

		UnitTestUtilities.startDefaultServer();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		UnitTestUtilities.stopDefaultServer();
		sc.stop();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
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

		StandardNatsSubscriber ns1 = new StandardNatsSubscriber(NATS_SERVER_URL, subject + "_id", subject, data.size());

		// start the subscribers apps
		executor.execute(ns1);

		// wait for subscribers to be ready.
		ns1.waitUntilReady();
		return ns1;
	}

	@Test(timeout=2000)
	public void testStaticSparkToNatsNoSubjects() throws Exception {   
		final List<String> data = getData();

		JavaRDD<String> rdd = sc.parallelize(data);
		
		try {
			rdd.foreach(SparkToNatsConnector.newConnection().withNatsURL(NATS_SERVER_URL).publishToNats());
		} catch (Exception e) {
			if (e.getMessage().contains("needs at least one NATS Subject"))
				return;
			else
				throw e;
		}	

		fail("An Exception(\"SparkToNatsConnector needs at least one Subject\") should have been raised.");
	}

	@Test(timeout=2000)
	public void testStaticSparkToNatsWithMultipleSubjects() throws Exception {   
		final List<String> data = getData();

		String subject1 = "subject1";
		StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, subject1);

		String subject2 = "subject2";
		StandardNatsSubscriber ns2 = getStandardNatsSubscriber(data, subject2);

		JavaRDD<String> rdd = sc.parallelize(data);

		final VoidFunction<String> publishToNats = SparkToNatsConnector.newConnection().withNatsURL(NATS_SERVER_URL).withSubjects(DEFAULT_SUBJECT, subject1, subject2).publishToNats();
		rdd.foreach(publishToNats);	

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}

	@Test(timeout=8000)
	public void testStaticSparkToNatsWithTimeout() throws Exception { 
	   	// TODO Replace the deleted assertions
		final List<String> data = getData();

		String subject1 = "subject1";
		StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, subject1);

		String subject2 = "subject2";
		StandardNatsSubscriber ns2 = getStandardNatsSubscriber(data, subject2);

		JavaRDD<String> rdd = sc.parallelize(data);
		
///		assertTrue("NO connections should be opened when entering the test", SparkToNatsConnector.CONNECTIONS.isEmpty());

		rdd.foreach(
				SparkToNatsConnector
					.newConnection().withNatsURL(NATS_SERVER_URL).withSubjects(DEFAULT_SUBJECT, subject1, subject2)
					.withConnectionTimeout(Duration.ofSeconds(2))
					.publishToNats());	

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
		
///		assertFalse("Some connections should have been opened", SparkToNatsConnector.CONNECTIONS.isEmpty());
		
		TimeUnit.SECONDS.sleep(5);
		
///		assertTrue("NO connections should be still opened when exiting the test", SparkToNatsConnector.CONNECTIONS.isEmpty());
	}

	@Test(timeout=2000)
	public void testStaticSparkToNatsWithProperties() throws Exception {   
		final List<String> data = getData();

		StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, DEFAULT_SUBJECT);

		JavaRDD<String> rdd = sc.parallelize(data);

		final Properties properties = new Properties();
		properties.setProperty(PROP_URL, NATS_SERVER_URL);
		properties.setProperty(PROP_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");

		rdd.foreach(SparkToNatsConnector.newConnection().withProperties(properties).publishToNats());		

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
	}
}
