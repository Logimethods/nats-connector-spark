/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.NATS_SERVER_URL;
import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static io.nats.client.Options.PROP_URL;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
//import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats.spark.test.StandardNatsSubscriber;
import com.logimethods.connector.nats.spark.test.TestClient;
import com.logimethods.connector.nats.spark.test.UnitTestUtilities;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnector;
import com.logimethods.connector.spark.to_nats.SparkToStandardNatsConnectorImpl;

import scala.Tuple2;

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
		Level level = Level.WARN;
		UnitTestUtilities.setLogLevel(SparkToNatsConnector.class, level);
		UnitTestUtilities.setLogLevel(SparkToStandardNatsConnectorImpl.class, level);
		UnitTestUtilities.setLogLevel(SparkToStandardNatsConnectorTest.class, level);
		UnitTestUtilities.setLogLevel(TestClient.class, level);
		UnitTestUtilities.setLogLevel("org.apache.spark", level);
		UnitTestUtilities.setLogLevel("org.spark-project", level);
		
		logger = LoggerFactory.getLogger(SparkToStandardNatsConnectorTest.class);       

		SparkConf sparkConf = new SparkConf().setAppName("My Spark Job").setMaster("local[2]").set("spark.driver.host", "localhost"); // https://issues.apache.org/jira/browse/
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
	 * @param data
	 * @return
	 */
/*	protected StandardNatsSubscriber getStandardNatsSubscriber(final List<String> data, String subject) {
		ExecutorService executor = Executors.newFixedThreadPool(1);

		StandardNatsSubscriber ns1 = new StandardNatsSubscriber(NATS_SERVER_URL, subject + "_id", subject, data.size());

		// start the subscribers apps
		executor.execute(ns1);

		// wait for subscribers to be ready.
		ns1.waitUntilReady();
		return ns1;
	}*/

	@Test(timeout=120000)
	public void testStaticSparkToNatsNoSubjects() throws Exception {   
		JavaRDD<Integer> rdd = UnitTestUtilities.getJavaRDD(sc);
		
		try {
			SparkToNatsConnector.newConnection().withNatsURL(NATS_SERVER_URL).publishToNats(rdd);
		} catch (Exception e) {
			if (e.getMessage().contains("needs at least one NATS Subject"))
				return;
			else
				throw e;
		}	

		fail("An Exception(\"SparkToNatsConnector needs at least one Subject\") should have been raised.");
	}

	@Test(timeout=120000)
	public void testStaticKeyValueSparkToNatsNoSubjects() throws Exception {   
		String subject1 = "subject1";

		JavaRDD<Tuple2<String, Integer>> stream = getKeyValueStream(subject1);		
		SparkToNatsConnector.newConnection().withNatsURL(NATS_SERVER_URL).publishToNatsAsKeyValue(stream);
	}

	protected JavaRDD<Tuple2<String, Integer>> getKeyValueStream(String subject1) {
		final List<Integer> data = UnitTestUtilities.getData();
		JavaRDD<Integer> rdd = sc.parallelize(data);
				
		JavaRDD<Tuple2<String, Integer>> stream = 
				rdd.map((Function<Integer, Tuple2<String, Integer>>) str -> {
									return new Tuple2<String, Integer>(subject1, str);
								});
		return stream;
	}

	@Test(timeout=120000)
	public void testStaticSparkToNatsWithMultipleSubjects() throws Exception {   
		final List<Integer> data = UnitTestUtilities.getData();

		String subject1 = "subject1";
		StandardNatsSubscriber ns1 = UnitTestUtilities.getStandardNatsSubscriber(data, subject1, NATS_SERVER_URL);

		String subject2 = "subject2";
		StandardNatsSubscriber ns2 = UnitTestUtilities.getStandardNatsSubscriber(data, subject2, NATS_SERVER_URL);

		JavaRDD<Integer> rdd = sc.parallelize(data);

		SparkToNatsConnector
					.newConnection()
					.withNatsURL(NATS_SERVER_URL)
					.withSubjects(DEFAULT_SUBJECT, subject1, subject2)
					.publishToNats(rdd);

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}

	@Test(timeout=120000)
	public void testStaticKeyValueSparkToNatsWithMultipleSubjects() throws Exception {   
		final List<Integer> data = UnitTestUtilities.getData();
		
		final String rootSubject = "ROOT";

		String subject1 = "subject1";
		StandardNatsSubscriber ns1 = UnitTestUtilities.getStandardNatsSubscriber(data, rootSubject + "." + subject1 + ".>", NATS_SERVER_URL);

		String subject2 = "subject2";
		StandardNatsSubscriber ns2 = UnitTestUtilities.getStandardNatsSubscriber(data, rootSubject + "." + subject2 + ".>", NATS_SERVER_URL);

		JavaRDD<Integer> rdd = sc.parallelize(data);
		JavaRDD<Tuple2<String, Integer>> stream1 = 
				rdd.map((Function<Integer, Tuple2<String, Integer>>) str -> {
									return new Tuple2<String, Integer>(subject1 + "." + str, str);
								});		
		JavaRDD<Tuple2<String, Integer>> stream2 = 
				rdd.map((Function<Integer, Tuple2<String, Integer>>) str -> {
									return new Tuple2<String, Integer>(subject2 + "." + str, str);
								});		
		JavaRDD<Tuple2<String, Integer>> stream = stream1.union(stream2);

		SparkToNatsConnector
			.newConnection()
			.withNatsURL(NATS_SERVER_URL)
			.withSubjects(rootSubject + ".")
			.publishToNatsAsKeyValue(stream);

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}

	@Test(timeout=120000)
	public void testStaticSparkToNatsWithProperties() throws Exception {   
		final List<Integer> data = UnitTestUtilities.getData();

		StandardNatsSubscriber ns1 = UnitTestUtilities.getStandardNatsSubscriber(data, DEFAULT_SUBJECT, NATS_SERVER_URL);

		JavaRDD<Integer> rdd = sc.parallelize(data);

		final Properties properties = new Properties();
		properties.setProperty(PROP_URL, NATS_SERVER_URL);
		properties.setProperty(PROP_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");

//		rdd.foreach(SparkToNatsConnector.newConnection().withProperties(properties).publishToNats());		
		SparkToNatsConnector.newConnection().withProperties(properties).publishToNats(rdd);		

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
	}
}
