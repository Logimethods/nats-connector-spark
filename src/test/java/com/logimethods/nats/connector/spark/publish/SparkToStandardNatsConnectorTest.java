/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark.publish;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.nats.connector.spark.StandardNatsSubscriber;
import com.logimethods.nats.connector.spark.TestClient;
import com.logimethods.nats.connector.spark.UnitTestUtilities;
import com.logimethods.nats.connector.spark.publish.SparkToNatsConnector;

//@Ignore
public class SparkToStandardNatsConnectorTest {

	protected static final String DEFAULT_SUBJECT = "spark2natsSubject";
	protected static final String DEFAULT_NATS_URL = "nats://localhost:4222";
	protected static JavaSparkContext sc;
	static Logger logger = null;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Enable tracing for debugging as necessary.
		UnitTestUtilities.setLogLevel(SparkToNatsConnector.class, Level.WARN);
		UnitTestUtilities.setLogLevel(SparkToStandardNatsConnectorImpl.class, Level.WARN);
		UnitTestUtilities.setLogLevel(SparkToStandardNatsConnectorTest.class, Level.WARN);
		UnitTestUtilities.setLogLevel(TestClient.class, Level.WARN);
		
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

		StandardNatsSubscriber ns1 = new StandardNatsSubscriber(subject + "_id", subject, data.size());

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
			rdd.foreach(SparkToNatsConnector.newConnection().publishToNats());
		} catch (Exception e) {
			if (e.getMessage().contains("SparkToNatsConnector needs at least one NATS Subject"))
				return;
			else
				throw e;
		}	

		fail("An Exception(\"SparkToNatsConnector needs at least one Subject\") should have been raised.");
	}

	@Test(timeout=2000)
	public void testStaticSparkToNatsIncludingMultipleSubjects() throws Exception {   
		final List<String> data = getData();

		String subject1 = "subject1";
		StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, subject1);

		String subject2 = "subject2";
		StandardNatsSubscriber ns2 = getStandardNatsSubscriber(data, subject2);

		JavaRDD<String> rdd = sc.parallelize(data);

		rdd.foreach(SparkToNatsConnector.publishToNats(DEFAULT_SUBJECT, subject1, subject2));		

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}

	@Test(timeout=2000)
	public void testStaticSparkToNatsWithMultipleSubjects() throws Exception {   
		final List<String> data = getData();

		String subject1 = "subject1";
		StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, subject1);

		String subject2 = "subject2";
		StandardNatsSubscriber ns2 = getStandardNatsSubscriber(data, subject2);

		JavaRDD<String> rdd = sc.parallelize(data);

		rdd.foreach(SparkToNatsConnector.newConnection().withSubjects(DEFAULT_SUBJECT, subject1, subject2).publishToNats());		

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}

	@Test(timeout=2000)
	public void testStaticSparkToNatsIncludingProperties() throws Exception {   
		final List<String> data = getData();

		StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, DEFAULT_SUBJECT);

		JavaRDD<String> rdd = sc.parallelize(data);

		final Properties properties = new Properties();
		properties.setProperty(SparkToNatsConnector.NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		rdd.foreach(SparkToNatsConnector.publishToNats(DEFAULT_NATS_URL, properties));		

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
	}

	@Test(timeout=2000)
	public void testStaticSparkToNatsWithProperties() throws Exception {   
		final List<String> data = getData();

		StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, DEFAULT_SUBJECT);

		JavaRDD<String> rdd = sc.parallelize(data);

		final Properties properties = new Properties();
		properties.setProperty(SparkToNatsConnector.NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		rdd.foreach(SparkToNatsConnector.newConnection().withProperties(properties).publishToNats());		

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
	}

	@Test(timeout=2000)
	public void testStaticSparkToNatsWithSystemProperties() throws Exception {   
		final List<String> data = getData();

		StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, DEFAULT_SUBJECT);

		JavaRDD<String> rdd = sc.parallelize(data);

		System.setProperty(SparkToNatsConnector.NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");

		try {
			rdd.foreach(SparkToNatsConnector.newConnection().publishToNats());
			// wait for the subscribers to complete.
			ns1.waitForCompletion();
		} finally {
			System.clearProperty(SparkToNatsConnector.NATS_SUBJECTS);
		}		
	}
}
