/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static io.nats.client.Constants.PROP_URL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.junit.Test;

import com.logimethods.connector.nats_spark.IncompleteException;

import io.nats.stan.SubscriptionOptions;
import scala.Tuple2;

public class NatsStreamingToKeyValueSparkWithAttributesTest {
	protected final static String CLUSTER_ID = "CLUSTER_ID";
	private static final int STANServerPORT = 4223;
	private static final String STAN_URL = "nats://localhost:" + STANServerPORT;
	private static final String ALT_STAN_URL = "nats://1.1.1.1:" + STANServerPORT;
	protected final static String DURABLE_NAME = "$DURABLE_NAME";
	protected final static Properties PROPERTIES = new Properties();
	
	{
		PROPERTIES.setProperty(PROP_SUBJECTS, "sub1,sub3 , sub2");
		PROPERTIES.setProperty(PROP_URL, STAN_URL);
	}

	@Test
	public void testNatsStreamingToKeyValueSparkConnectorImpl_0() throws IncompleteException {
		NatsStreamingToKeyValueSparkConnectorImpl connector = 
				NatsToSparkConnector
					.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
					.withProperties(PROPERTIES)
					.storedAsKeyValue();
		assertTrue(connector instanceof NatsStreamingToKeyValueSparkConnectorImpl);
		assertEquals(connector.getNatsUrl().toString(), STAN_URL, connector.getNatsUrl());
		assertEquals(connector.getSubjects().toString(), 3, connector.getSubjects().size());
	}

	@Test
	public void testNatsStreamingToKeyValueSparkConnectorImpl_1() throws IncompleteException {
		NatsStreamingToKeyValueSparkConnectorImpl connector = 
				NatsToSparkConnector
					.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
					.withNatsURL(ALT_STAN_URL)
					.withSubjects("sub1", "sub2")
					.storedAsKeyValue();
		assertTrue(connector instanceof NatsStreamingToKeyValueSparkConnectorImpl);
		assertEquals(ALT_STAN_URL, connector.natsUrl);
		assertEquals(connector.getSubjects().toString(), 2, connector.getSubjects().size());
	}

	@Test
	public void testNatsStreamingToKeyValueSparkConnectorImpl_1_1() throws IncompleteException {
		NatsStreamingToKeyValueSparkConnectorImpl connector = 
				NatsToSparkConnector
					.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
					.withNatsURL(ALT_STAN_URL)
					.withSubjects("sub1", "sub2")
					.withProperties(PROPERTIES)
					.storedAsKeyValue();
		assertTrue(connector instanceof NatsStreamingToKeyValueSparkConnectorImpl);
		assertEquals(ALT_STAN_URL, connector.natsUrl);
		assertEquals(connector.getSubjects().toString(), 2, connector.getSubjects().size());
	}

	@Test
	public void testNatsStreamingToKeyValueSparkConnectorImpl_2() {
		SubscriptionOptions.Builder optsBuilder = new SubscriptionOptions.Builder().setDurableName(DURABLE_NAME);
		NatsStreamingToKeyValueSparkConnectorImpl connector = 
				NatsToSparkConnector
					.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
					.withNatsURL(STAN_URL)
					.withSubscriptionOptionsBuilder(optsBuilder)
					.withSubjects("SUBJECT")
					.storedAsKeyValue();
		assertTrue(connector instanceof NatsStreamingToKeyValueSparkConnectorImpl);
		assertEquals(DURABLE_NAME, connector.getSubscriptionOptions().getDurableName());
	}

	@Test
	public void testNatsStreamingToKeyValueSparkConnectorImpl_3() {
		NatsStreamingToKeyValueSparkConnectorImpl connector = 
				NatsToSparkConnector
					.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
					.withNatsURL(STAN_URL)
					.startWithLastReceived()
					.setDurableName(DURABLE_NAME)
					.withSubjects("SUBJECT")
					.storedAsKeyValue();
		assertTrue(connector instanceof NatsStreamingToKeyValueSparkConnectorImpl);
		assertEquals(DURABLE_NAME, connector.getSubscriptionOptions().getDurableName());
	}

	@Test
	public void testNatsStreamingToKeyValueSparkConnectorImpl_4() {
		final Instant start = Instant.now().minus(30, ChronoUnit.MINUTES);
		SubscriptionOptions.Builder optsBuilder = new SubscriptionOptions.Builder().setDurableName(DURABLE_NAME).startAtTime(start);
		final String newName = "NEW NAME";
		NatsStreamingToKeyValueSparkConnectorImpl connector = 
				NatsToSparkConnector
					.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
					.withNatsURL(STAN_URL)
					//.withProperties(PROPERTIES)
					.withSubscriptionOptionsBuilder(optsBuilder)
					.setDurableName(newName)
					.withSubjects("SUBJECT")
					.storedAsKeyValue();
		assertTrue(connector instanceof NatsStreamingToKeyValueSparkConnectorImpl);
		assertEquals(newName, connector.getSubscriptionOptions().getDurableName());
		assertEquals(start, connector.getSubscriptionOptions().getStartTime());
	}

	@Test
	public void testNatsStreamingToKeyValueSparkConnectorImpl_5() {
		final Instant start = Instant.now().minus(30, ChronoUnit.MINUTES);
		NatsStreamingToKeyValueSparkConnectorImpl connector = 
				NatsToSparkConnector
					.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
					.withNatsURL(STAN_URL)
					//.withProperties(PROPERTIES)
					.setDurableName(DURABLE_NAME)
					.startAtTime(start)
					.withSubjects("SUBJECT")
					.storedAsKeyValue();
		assertTrue(connector instanceof NatsStreamingToKeyValueSparkConnectorImpl);
		assertEquals(DURABLE_NAME, connector.getSubscriptionOptions().getDurableName());
		assertEquals(start, connector.getSubscriptionOptions().getStartTime());
	}
}
