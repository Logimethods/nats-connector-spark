/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.NATS_SERVER_URL;
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

import com.logimethods.connector.nats.spark.test.StandardNatsSubscriber;
import com.logimethods.connector.nats.spark.test.TestClient;
import com.logimethods.connector.nats.spark.test.UnitTestUtilities;

//@Ignore
@SuppressWarnings("serial")
public abstract class AbstractSparkToStandardNatsConnectorLifecycleTest extends AbstractSparkToNatsConnectorTest {

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
		UnitTestUtilities.setLogLevel(AbstractSparkToStandardNatsConnectorLifecycleTest.class, level);
		UnitTestUtilities.setLogLevel(TestClient.class, level);
		UnitTestUtilities.setLogLevel("org.apache.spark", Level.WARN);
		UnitTestUtilities.setLogLevel("org.spark-project", Level.WARN);
	
		logger = LoggerFactory.getLogger(AbstractSparkToStandardNatsConnectorLifecycleTest.class);       
		
		UnitTestUtilities.startDefaultServer();
	}

	@Test(timeout=120000)
	public void testStaticSparkToNatsWithConnectionLifecycle() throws Exception {  
		final long poolSize = SparkToStandardNatsConnectorPool.poolSize();
		
		final List<Integer> data = UnitTestUtilities.getData();

		final String subject1 = "subject1";

		final String subject2 = "subject2";

		final int partitionsNb = 3;
		publishToNats(subject1, subject2, partitionsNb);
		
		ssc.start();

		TimeUnit.SECONDS.sleep(1);

		final StandardNatsSubscriber ns1 = UnitTestUtilities.getStandardNatsSubscriber(data, subject1, NATS_SERVER_URL);
		final StandardNatsSubscriber ns2 = UnitTestUtilities.getStandardNatsSubscriber(data, subject2, NATS_SERVER_URL);
		writeTmpFile(data);
		// wait for the subscribers to complete.
		ns1.waitForCompletion();
		ns2.waitForCompletion();
		
		TimeUnit.MILLISECONDS.sleep(100);
		assertEquals("The connections Pool size should be the same as the number of Spark partitions", 
				poolSize + partitionsNb, SparkToStandardNatsConnectorPool.poolSize());
				
		final StandardNatsSubscriber ns1p = UnitTestUtilities.getStandardNatsSubscriber(data, subject1, NATS_SERVER_URL);
		final StandardNatsSubscriber ns2p = UnitTestUtilities.getStandardNatsSubscriber(data, subject2, NATS_SERVER_URL);

		writeTmpFile(data);
		// wait for the subscribers to complete.
		ns1p.waitForCompletion();
		ns2p.waitForCompletion();
		TimeUnit.MILLISECONDS.sleep(100);
		assertEquals("The connections Pool size should be the same as the number of Spark partitions", 
				poolSize + partitionsNb, SparkToStandardNatsConnectorPool.poolSize());

		ssc.stop();
		ssc = null;
		
		logger.debug("Spark Context Stopped");
		
		TimeUnit.SECONDS.sleep(5);
		logger.debug("After 5 sec delay");
		
		assertTrue("The poolSize() of " + SparkToStandardNatsConnectorPool.connectorsPoolMap + " should have been reverted to its original value",
				SparkToStandardNatsConnectorPool.poolSize() == poolSize);
	}

	protected abstract void publishToNats(final String subject1, final String subject2, final int partitionsNb);
}
