/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import static com.logimethods.connector.nats.spark.UnitTestUtilities.NATS_SERVER_URL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats.spark.StandardNatsSubscriber;
import com.logimethods.connector.nats.spark.TestClient;
import com.logimethods.connector.nats.spark.UnitTestUtilities;

//@Ignore
@SuppressWarnings("serial")
public class SparkToStandardNatsConnectorLifecycleTest extends AbstractSparkToNatsConnectorTest {

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
		UnitTestUtilities.setLogLevel(SparkToStandardNatsConnectorLifecycleTest.class, level);
		UnitTestUtilities.setLogLevel(TestClient.class, level);
		UnitTestUtilities.setLogLevel("org.apache.spark", Level.WARN);
		UnitTestUtilities.setLogLevel("org.spark-project", Level.WARN);
	
		logger = LoggerFactory.getLogger(SparkToStandardNatsConnectorLifecycleTest.class);       
		
		UnitTestUtilities.startDefaultServer();
	}

	@Test(timeout=20000)
	public void testStaticSparkToNatsWithConnectionLifecycle() throws Exception {  
		final long poolSize = SparkToStandardNatsConnectorPool.poolSize();
		final int connectionsPoolKeysNb = SparkToStandardNatsConnectorPool.connectionsPoolMap.size();
		final int connectorsByIdMapKeysNb = SparkToStandardNatsConnectorPool.connectorsByIdMap.size();
		
		final List<String> data = getData();

		final String subject1 = "subject1";

		final String subject2 = "subject2";

		final int partitionsNb = 3;
		final JavaDStream<String> lines = ssc.textFileStream(tempDir.getAbsolutePath()).repartition(partitionsNb);		
		
		SparkToNatsConnectorPool
			.newPool()
			.withNatsURL(NATS_SERVER_URL)
			.withConnectionTimeout(Duration.ofSeconds(2))
			.withSubjects(DEFAULT_SUBJECT, subject1, subject2)
			.publishToNats(lines);
		
		ssc.start();

		TimeUnit.SECONDS.sleep(1);

		final StandardNatsSubscriber ns1 = getStandardNatsSubscriber(data, subject1);
		final StandardNatsSubscriber ns2 = getStandardNatsSubscriber(data, subject2);
		writeTmpFile(data);
		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
		
		TimeUnit.MILLISECONDS.sleep(100);
		assertEquals("The connections Pool size should be the same as the number of Spark partitions", 
				poolSize + partitionsNb, SparkToStandardNatsConnectorPool.poolSize());
		assertEquals("The connectionsPoolMap " + SparkToStandardNatsConnectorPool.connectionsPoolMap + " should contain ONE extra key", 
				connectionsPoolKeysNb + 1, SparkToStandardNatsConnectorPool.connectionsPoolMap.size());
		assertEquals("The connectorsByIdMap " + SparkToStandardNatsConnectorPool.connectorsByIdMap + " should not contain anymore extra keys", 
				connectorsByIdMapKeysNb, SparkToStandardNatsConnectorPool.connectorsByIdMap.size());
				
		final StandardNatsSubscriber ns1p = getStandardNatsSubscriber(data, subject1);
		final StandardNatsSubscriber ns2p = getStandardNatsSubscriber(data, subject2);

		writeTmpFile(data);
		// wait for the subscribers to complete.
		ns1p.waitForCompletion();
		ns2p.waitForCompletion();
		TimeUnit.MILLISECONDS.sleep(100);
		assertEquals("The connections Pool size should be the same as the number of Spark partitions", 
				poolSize + partitionsNb, SparkToStandardNatsConnectorPool.poolSize());
		assertEquals("The connectionsPoolMap " + SparkToStandardNatsConnectorPool.connectionsPoolMap + " should contain ONE extra key", 
				connectionsPoolKeysNb + 1, SparkToStandardNatsConnectorPool.connectionsPoolMap.size());
		assertEquals("The connectorsByIdMap " + SparkToStandardNatsConnectorPool.connectorsByIdMap + " should not contain anymore extra keys", 
				connectorsByIdMapKeysNb, SparkToStandardNatsConnectorPool.connectorsByIdMap.size());

		ssc.stop();
		ssc = null;
		
		logger.debug("Spark Context Stopped");
		
		TimeUnit.SECONDS.sleep(5);
		logger.debug("After 5 sec delay");
		
		assertTrue("The poolSize() of " + SparkToStandardNatsConnectorPool.connectionsPoolMap + " should have been reverted to its original value",
				SparkToStandardNatsConnectorPool.poolSize() == poolSize);
		assertEquals("The connectionsPoolMap " + SparkToStandardNatsConnectorPool.connectionsPoolMap + " should not contain anymore extra keys", 
				connectionsPoolKeysNb, SparkToStandardNatsConnectorPool.connectionsPoolMap.size());
		assertEquals("The connectorsByIdMap " + SparkToStandardNatsConnectorPool.connectorsByIdMap + " should not contain anymore extra keys", 
				connectorsByIdMapKeysNb, SparkToStandardNatsConnectorPool.connectorsByIdMap.size());
	}
}
