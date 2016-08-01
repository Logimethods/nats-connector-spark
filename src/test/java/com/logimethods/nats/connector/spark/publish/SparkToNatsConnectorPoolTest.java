/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark.publish;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.log4j.Level.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
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
import com.logimethods.nats.connector.spark.NatsSubscriber;
import com.logimethods.nats.connector.spark.TestClient;
import com.logimethods.nats.connector.spark.UnitTestUtilities;
import com.logimethods.nats.connector.spark.publish.SparkToNatsConnector;
import com.logimethods.nats.connector.spark.publish.SparkToNatsConnectorPool;

//@Ignore
@SuppressWarnings("serial")
public class SparkToNatsConnectorPoolTest implements Serializable {

	protected static final String DEFAULT_SUBJECT = "spark2natsSubject";
	static JavaStreamingContext ssc;
	static Logger logger = null;
	File tempDir;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Enable tracing for debugging as necessary.
		UnitTestUtilities.setLogLevel(SparkToNatsConnector.class, TRACE);
		UnitTestUtilities.setLogLevel(SparkToStandardNatsConnectorImpl.class, INFO);
		UnitTestUtilities.setLogLevel(SparkToNatsConnectorPool.class, INFO);
		UnitTestUtilities.setLogLevel(SparkToNatsConnectorPoolTest.class, INFO);
		UnitTestUtilities.setLogLevel(TestClient.class, INFO);

		logger = LoggerFactory.getLogger(SparkToNatsConnectorPoolTest.class);       
		
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
//		assertTrue(logger.isDebugEnabled());
//		assertTrue(LoggerFactory.getLogger(SparkToNatsConnector.class).isTraceEnabled());

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
	protected NatsSubscriber getNatsSubscriber(final List<String> data, String subject) {
		ExecutorService executor = Executors.newFixedThreadPool(1);

		NatsSubscriber ns1 = new NatsSubscriber(subject + "_id", subject, data.size());

		// start the subscribers apps
		executor.execute(ns1);

		// wait for subscribers to be ready.
		ns1.waitUntilReady();
		return ns1;
	}

/*	@Test(timeout=6000)
	public void testStaticSparkToNatsNoSubjects() throws Exception {   
		final List<String> data = getData();

		JavaRDD<String> rdd = sc.parallelize(data);

		try {
			rdd.foreach(SparkToNatsConnector.publishToNats());
		} catch (Exception e) {
			if (e.getMessage().contains("SparkToNatsConnector needs at least one NATS Subject"))
				return;
			else
				throw e;
		}	

		fail("An Exception(\"SparkToNatsConnector needs at least one Subject\") should have been raised.");
	}*/

	@Test(timeout=8000)
	public void testStaticSparkToNatsWithMultipleSubjects() throws Exception {   
		final List<String> data = getData();

		String subject1 = "subject1";
		NatsSubscriber ns1 = getNatsSubscriber(data, subject1);

		String subject2 = "subject2";
		NatsSubscriber ns2 = getNatsSubscriber(data, subject2);

		JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

final SparkToNatsConnectorPool connectorPool = new SparkToNatsConnectorPool(DEFAULT_SUBJECT, subject1, subject2);
lines.foreachRDD(new Function<JavaRDD<String>, Void> (){
	@Override
	public Void call(JavaRDD<String> rdd) throws Exception {
		final SparkToNatsConnector connector = connectorPool.getConnector();
		rdd.foreachPartition(new VoidFunction<Iterator<String>> (){
			@Override
			public void call(Iterator<String> strings) throws Exception {
				while(strings.hasNext()) {
					final String str = strings.next();
					logger.debug("Will publish " + str);
					connector.publish(str);
				}
			}
		});
		connectorPool.returnConnector(connector);  // return to the pool for future reuse
		return null;
	}			
});

//		lines.print();
		
		ssc.start();

		Thread.sleep(1000);

		File tmpFile = new File(tempDir.getAbsolutePath(), "tmp.txt");
		PrintWriter writer = new PrintWriter(tmpFile, "UTF-8");
		for(String str: data) {
			writer.println(str);
		}		
		writer.close();

//		Thread.sleep(6000);

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}

/*	@Test(timeout=6000)
	public void testStaticSparkToNatsWithProperties() throws Exception {   
		final List<String> data = getData();

		NatsSubscriber ns1 = getNatsSubscriber(data, DEFAULT_SUBJECT);

		JavaRDD<String> rdd = sc.parallelize(data);

		final Properties properties = new Properties();
		properties.setProperty(SparkToNatsConnector.NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		rdd.foreach(SparkToNatsConnector.publishToNats(properties));		

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
	}

	@Test(timeout=6000)
	public void testStaticSparkToNatsWithSystemProperties() throws Exception {   
		final List<String> data = getData();

		NatsSubscriber ns1 = getNatsSubscriber(data, DEFAULT_SUBJECT);

		JavaRDD<String> rdd = sc.parallelize(data);

		System.setProperty(SparkToNatsConnector.NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");

		try {
			rdd.foreach(SparkToNatsConnector.publishToNats());
			// wait for the subscribers to complete.
			ns1.waitForCompletion();
		} finally {
			System.clearProperty(SparkToNatsConnector.NATS_SUBJECTS);
		}		
	}*/
}
