/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
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
import com.logimethods.connector.nats.spark.StandardNatsSubscriber;
import com.logimethods.connector.nats.spark.TestClient;
import com.logimethods.connector.nats.spark.UnitTestUtilities;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnector;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnectorPool;
import com.logimethods.connector.spark.to_nats.SparkToStandardNatsConnectorImpl;
import com.logimethods.connector.spark.to_nats.SparkToStandardNatsConnectorPool;

//@Ignore
@SuppressWarnings("serial")
public class SparkToStandardNatsConnectorPoolTest implements Serializable {

	protected static final String DEFAULT_SUBJECT = "spark2natsSubject";
	public static final String NATS_URL = "nats://localhost:4222";
	static JavaStreamingContext ssc;
	static Logger logger = null;
	File tempDir;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
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

		StandardNatsSubscriber ns1 = new StandardNatsSubscriber(NATS_URL, subject + "_id", subject, data.size());

		// start the subscribers apps
		executor.execute(ns1);

		// wait for subscribers to be ready.
		ns1.waitUntilReady();
		return ns1;
	}

	@SuppressWarnings("deprecation")
	@Test(timeout=8000)
	public void testStaticSparkToNatsIncludingMultipleSubjects() throws Exception {   
		final List<String> data = getData();

		String subject1 = "subject1";
		StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, subject1);

		String subject2 = "subject2";
		StandardNatsSubscriber ns2 = getStandardNatsSubscriber(data, subject2);

		JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

		final SparkToNatsConnectorPool<?> connectorPool = new SparkToStandardNatsConnectorPool().withSubjects(DEFAULT_SUBJECT, subject1, subject2);
		lines.foreachRDD(new Function<JavaRDD<String>, Void> (){
			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
				final SparkToNatsConnector<?> connector = connectorPool.getConnector();
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
		
		ssc.start();

		Thread.sleep(1000);

		File tmpFile = new File(tempDir.getAbsolutePath(), "tmp.txt");
		PrintWriter writer = new PrintWriter(tmpFile, "UTF-8");
		for(String str: data) {
			writer.println(str);
		}		
		writer.close();

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}

	@SuppressWarnings("deprecation")
	@Test(timeout=8000)
	public void testStaticSparkToNatsWithMultipleSubjects() throws Exception {   
		final List<String> data = getData();

		String subject1 = "subject1";
		StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, subject1);

		String subject2 = "subject2";
		StandardNatsSubscriber ns2 = getStandardNatsSubscriber(data, subject2);

		JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

		final SparkToNatsConnectorPool<?> connectorPool = SparkToNatsConnectorPool.newPool().withSubjects(DEFAULT_SUBJECT, subject1, subject2);
		lines.foreachRDD(new Function<JavaRDD<String>, Void> (){
			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
				final SparkToNatsConnector<?> connector = connectorPool.getConnector();
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
		
		ssc.start();

		Thread.sleep(1000);

		File tmpFile = new File(tempDir.getAbsolutePath(), "tmp.txt");
		PrintWriter writer = new PrintWriter(tmpFile, "UTF-8");
		for(String str: data) {
			writer.println(str);
		}		
		writer.close();

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}

	@SuppressWarnings("deprecation")
	@Test(timeout=8000)
	public void testStaticSparkToNatsWithMultipleProperties() throws Exception {   
		final List<String> data = getData();

		String subject1 = "subject1";
		StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, subject1);

		String subject2 = "subject2";
		StandardNatsSubscriber ns2 = getStandardNatsSubscriber(data, subject2);

		JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

		final Properties properties = new Properties();
		properties.setProperty(SparkToNatsConnector.NATS_SUBJECTS, subject1+","+DEFAULT_SUBJECT+" , "+subject2);

		final SparkToNatsConnectorPool<?> connectorPool = SparkToNatsConnectorPool.newPool().withProperties(properties);
		lines.foreachRDD(new Function<JavaRDD<String>, Void> (){
			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
				final SparkToNatsConnector<?> connector = connectorPool.getConnector();
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
		
		ssc.start();

		Thread.sleep(1000);

		File tmpFile = new File(tempDir.getAbsolutePath(), "tmp.txt");
		PrintWriter writer = new PrintWriter(tmpFile, "UTF-8");
		for(String str: data) {
			writer.println(str);
		}		
		writer.close();

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}
}
