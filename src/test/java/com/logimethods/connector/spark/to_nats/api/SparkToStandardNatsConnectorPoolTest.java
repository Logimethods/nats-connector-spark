/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats.api;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.NATS_SERVER_URL;
import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static com.logimethods.connector.nats_spark.NatsSparkUtilities.encodeData;
import static io.nats.client.Constants.PROP_URL;

import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats.spark.test.StandardNatsSubscriber;
import com.logimethods.connector.nats.spark.test.TestClient;
import com.logimethods.connector.nats.spark.test.UnitTestUtilities;
import com.logimethods.connector.nats_spark.NatsSparkUtilities;
import com.logimethods.connector.spark.to_nats.AbstractSparkToNatsConnectorTest;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnector;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnectorPool;
import com.logimethods.connector.spark.to_nats.SparkToStandardNatsConnectorImpl;

import scala.Tuple2;

//@Ignore
@SuppressWarnings("serial")
public class SparkToStandardNatsConnectorPoolTest extends AbstractSparkToNatsConnectorTest {

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
		UnitTestUtilities.setLogLevel(SparkToStandardNatsConnectorPoolTest.class, level);
		UnitTestUtilities.setLogLevel(TestClient.class, level);
		UnitTestUtilities.setLogLevel("org.apache.spark", Level.WARN);
		UnitTestUtilities.setLogLevel("org.spark-project", Level.WARN);

		logger = LoggerFactory.getLogger(SparkToStandardNatsConnectorPoolTest.class);       
		
		UnitTestUtilities.startDefaultServer();
	}

	@Test(timeout=8000)
	public void testStaticSparkToNatsIncludingMultipleSubjects() throws Exception {   
		final List<Integer> data = UnitTestUtilities.getData();

		final String subject1 = "subject1";
		final StandardNatsSubscriber ns1 = UnitTestUtilities.getStandardNatsSubscriber(data, subject1, NATS_SERVER_URL);

		final String subject2 = "subject2";
		final StandardNatsSubscriber ns2 = UnitTestUtilities.getStandardNatsSubscriber(data, subject2, NATS_SERVER_URL);

		final JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

		SparkToNatsConnectorPool.newPool().withSubjects(DEFAULT_SUBJECT, subject1, subject2).withNatsURL(NATS_SERVER_URL).publishToNats(lines);
		
		ssc.start();

		Thread.sleep(1000);

		writeTmpFile(data);

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}

	@Test(timeout=8000)
	public void testStaticSparkStoredAsKeyValueToNats() throws Exception {   
		final List<Integer> data = UnitTestUtilities.getData();

		final String subject1 = "subject1";
		final StandardNatsSubscriber ns1 = UnitTestUtilities.getStandardNatsSubscriber(data, subject1 + ".>", NATS_SERVER_URL);

		JavaPairDStream<String, String> keyValues = UnitTestUtilities.getJavaPairDStream(tempDir, ssc, subject1);		

		SparkToNatsConnectorPool.newPool()
								.storedAsKeyValue()
								.withNatsURL(NATS_SERVER_URL)
								.publishToNats(keyValues, (java.util.function.Function<String,  byte[]> & Serializable) str -> str.getBytes());
		
		ssc.start();

		Thread.sleep(1000);

		writeTmpFile(data);

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
	}

	@Test(timeout=8000)
	public void testStaticSparkToNatsWithMultipleSubjects() throws Exception {   
		final List<Integer> data = UnitTestUtilities.getData();

		final String subject1 = "subject1";
		final StandardNatsSubscriber ns1 = UnitTestUtilities.getStandardNatsSubscriber(data, subject1, NATS_SERVER_URL);

		final String subject2 = "subject2";
		final StandardNatsSubscriber ns2 = UnitTestUtilities.getStandardNatsSubscriber(data, subject2, NATS_SERVER_URL);

		final JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

		SparkToNatsConnectorPool.newPool().withSubjects(DEFAULT_SUBJECT, subject1, subject2).withNatsURL(NATS_SERVER_URL).publishToNats(lines);
		
		ssc.start();

		Thread.sleep(1000);

		writeTmpFile(data);

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}

	@Test(timeout=8000)
	public void testStaticSparkToNatsWithMultipleProperties() throws Exception {   
		final List<Integer> data = UnitTestUtilities.getData();

		final String subject1 = "subject1";
		final StandardNatsSubscriber ns1 = UnitTestUtilities.getStandardNatsSubscriber(data, subject1, NATS_SERVER_URL);

		final String subject2 = "subject2";
		final StandardNatsSubscriber ns2 = UnitTestUtilities.getStandardNatsSubscriber(data, subject2, NATS_SERVER_URL);

		final JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath());

		final Properties properties = new Properties();
		properties.setProperty(PROP_URL, NATS_SERVER_URL);
		properties.setProperty(PROP_SUBJECTS, subject1+","+DEFAULT_SUBJECT+" , "+subject2);

		SparkToNatsConnectorPool.newPool().withProperties(properties).publishToNats(lines);
		
		ssc.start();

		Thread.sleep(1000);

		writeTmpFile(data);

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
	}
}
