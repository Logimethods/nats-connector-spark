/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import static com.logimethods.connector.nats.spark.UnitTestUtilities.NATS_SERVER_URL;
import static com.logimethods.connector.nats.to_spark.NatsToSparkConnector.NATS_SUBJECTS;
import static org.junit.Assert.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.junit.Test;

import com.logimethods.connector.nats.to_spark.StandardNatsToSparkConnectorImpl;
import com.logimethods.connector.nats_spark.IncompleteException;
import com.logimethods.connector.nats.to_spark.NatsStreamingToSparkConnectorImpl;
import com.logimethods.connector.nats.to_spark.NatsToSparkConnector;

import io.nats.stan.SubscriptionOptions;
import static io.nats.client.Constants.*;

public class StandardNatsToSparkWithAttributesTest {
	protected final static String CLUSTER_ID = "CLUSTER_ID";
	protected final static String CLIENT_ID = "CLIENT_ID";
	private static final int STANServerPORT = 4223;
	private static final String STAN_URL = "nats://localhost:" + STANServerPORT;
	protected final static String DURABLE_NAME = "$DURABLE_NAME";
	protected final static Properties PROPERTIES = new Properties();
	
	{
		PROPERTIES.setProperty(NATS_SUBJECTS, "sub1,sub3 , sub2");
		PROPERTIES.setProperty(PROP_URL, STAN_URL);
	}

	@Test
	public void testNatsStandardToSparkConnectorImpl_1() {
		StandardNatsToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY())
				.withProperties(PROPERTIES).withSubjects("SUBJECT");
		assertTrue(connector instanceof StandardNatsToSparkConnectorImpl);
		// TODO Check natsURL
	}

	@Test
	public void testNatsStandardToSparkConnectorImpl_2() {
		StandardNatsToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY())
			.withSubjects("SUBJECT").withProperties(PROPERTIES);
		assertTrue(connector instanceof StandardNatsToSparkConnectorImpl);
	}

	@Test
	public void testNatsStreamingToSparkConnectorImpl_1() {
		NatsStreamingToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNatsStreaming(StorageLevel.MEMORY_ONLY(), CLUSTER_ID, CLIENT_ID)
				.withNatsURL(STAN_URL).withProperties(PROPERTIES).withSubjects("SUBJECT");
		assertTrue(connector instanceof NatsStreamingToSparkConnectorImpl);
	}

	@Test
	public void testNatsStreamingToSparkConnectorImpl_2() {
		SubscriptionOptions.Builder optsBuilder = new SubscriptionOptions.Builder().setDurableName(DURABLE_NAME);
		NatsStreamingToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNatsStreaming(StorageLevel.MEMORY_ONLY(), CLUSTER_ID, CLIENT_ID)
				.withNatsURL(STAN_URL).withProperties(PROPERTIES).withSubscriptionOptionsBuilder(optsBuilder).withSubjects("SUBJECT");
		assertTrue(connector instanceof NatsStreamingToSparkConnectorImpl);
		assertEquals(DURABLE_NAME, connector.getSubscriptionOptions().getDurableName());
	}

	@Test
	public void testNatsStreamingToSparkConnectorImpl_3() {
		NatsStreamingToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNatsStreaming(StorageLevel.MEMORY_ONLY(), CLUSTER_ID, CLIENT_ID)
				.withNatsURL(STAN_URL).startWithLastReceived().setDurableName(DURABLE_NAME ).withSubjects("SUBJECT");
		assertTrue(connector instanceof NatsStreamingToSparkConnectorImpl);
		assertEquals(DURABLE_NAME, connector.getSubscriptionOptions().getDurableName());
	}

	@Test
	public void testNatsStreamingToSparkConnectorImpl_4() {
		final Instant start = Instant.now().minus(30, ChronoUnit.MINUTES);
		SubscriptionOptions.Builder optsBuilder = new SubscriptionOptions.Builder().setDurableName(DURABLE_NAME).startAtTime(start);
		final String newName = "NEW NAME";
		NatsStreamingToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNatsStreaming(StorageLevel.MEMORY_ONLY(), CLUSTER_ID, CLIENT_ID)
				.withNatsURL(STAN_URL).withProperties(PROPERTIES).withSubscriptionOptionsBuilder(optsBuilder).setDurableName(newName).withSubjects("SUBJECT");
		assertTrue(connector instanceof NatsStreamingToSparkConnectorImpl);
		assertEquals(newName, connector.getSubscriptionOptions().getDurableName());
		assertEquals(start, connector.getSubscriptionOptions().getStartTime());
	}
	
	/**
	 * Test method for {@link com.logimethods.connector.nats.to_spark.NatsToSparkConnector#receiveFromNats(java.lang.String, int, java.lang.String)}.
	 * @throws Exception 
	 */
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWITHOUTSubjects() throws Exception {
		
		try {
			NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).withNatsURL(NATS_SERVER_URL).receive();
		} catch (IncompleteException e) {
			e.printStackTrace();
			return;
		}	

		fail("An Exception(\"NatsToSparkConnector needs at least one Subject\") should have been raised.");
	}

}
