/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import static com.logimethods.connector.nats.spark.UnitTestUtilities.NATS_SERVER_URL;
import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
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
	public void testNatsStandardToSparkConnectorImpl_0() throws IncompleteException {
		StandardNatsToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY())
				.withProperties(PROPERTIES);
		assertTrue(connector instanceof StandardNatsToSparkConnectorImpl);
		assertEquals(STAN_URL, connector.getEnrichedProperties().getProperty(PROP_URL));
		assertEquals(3, connector.getSubjects().size());
	}

	@Test
	public void testNatsStandardToSparkConnectorImpl_1() throws IncompleteException {
		StandardNatsToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY())
				.withProperties(PROPERTIES).withSubjects("SUBJECT");
		assertTrue(connector instanceof StandardNatsToSparkConnectorImpl);
		assertEquals(STAN_URL, connector.getEnrichedProperties().getProperty(PROP_URL));
		assertEquals(1, connector.getSubjects().size());
	}

	@Test
	public void testNatsStandardToSparkConnectorImpl_2() throws IncompleteException {
		StandardNatsToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY())
			.withSubjects("SUBJECT").withProperties(PROPERTIES);
		assertTrue(connector instanceof StandardNatsToSparkConnectorImpl);
		assertEquals(STAN_URL, connector.getEnrichedProperties().getProperty(PROP_URL));
		assertEquals(1, connector.getSubjects().size());
	}
	
	/**
	 * Test method for {@link com.logimethods.connector.nats.to_spark.NatsToSparkConnector#receiveFromNats(java.lang.String, int, java.lang.String)}.
	 * @throws Exception 
	 */
	@Test(timeout=6000, expected=IncompleteException.class)
	public void testNatsToSparkConnectorWITHOUTSubjects() throws Exception {
		NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).withNatsURL(NATS_SERVER_URL).receive();
	}
}
