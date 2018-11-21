/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.NATS_SERVER_URL;
import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static io.nats.client.Options.PROP_URL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.junit.Test;

import com.logimethods.connector.nats_spark.IncompleteException;

public class StandardNatsToSparkWithAttributesTest {
	protected final static String CLUSTER_ID = "CLUSTER_ID";
	private static final int STANServerPORT = 4223;
	private static final String STAN_URL = "nats://localhost:" + STANServerPORT;
	protected final static String DURABLE_NAME = "$DURABLE_NAME";
	protected final static Properties PROPERTIES = new Properties();
	
	{
		PROPERTIES.setProperty(PROP_SUBJECTS, "sub1,sub3 , sub2");
		PROPERTIES.setProperty(PROP_URL, STAN_URL);
	}

	@Test
	public void testNatsStandardToSparkConnectorImpl_0() throws IncompleteException {
		StandardNatsToSparkConnectorImpl<String> connector = 
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY())
					.withProperties(PROPERTIES);
		assertTrue(connector instanceof StandardNatsToSparkConnectorImpl);
		assertEquals(STAN_URL, connector.getEnrichedProperties().getProperty(PROP_URL));
		assertEquals(3, connector.getSubjects().size());
	}

	@Test
	public void testNatsStandardToSparkConnectorImpl_1() throws IncompleteException {
		StandardNatsToSparkConnectorImpl<String> connector = 
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY())
					.withProperties(PROPERTIES)
					.withSubjects("SUBJECT");
		assertTrue(connector instanceof StandardNatsToSparkConnectorImpl);
		assertEquals(STAN_URL, connector.getEnrichedProperties().getProperty(PROP_URL));
		assertEquals(1, connector.getSubjects().size());
	}

	@Test
	public void testNatsStandardToSparkConnectorImpl_2() throws IncompleteException {
		StandardNatsToSparkConnectorImpl<String> connector = 
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY())
					.withSubjects("SUBJECT")
					.withProperties(PROPERTIES);
		assertTrue(connector instanceof StandardNatsToSparkConnectorImpl);
		assertEquals(STAN_URL, connector.getEnrichedProperties().getProperty(PROP_URL));
		assertEquals(1, connector.getSubjects().size());
	}
	
	/**
	 * Test method for {@link com.logimethods.connector.nats.to_spark.NatsToSparkConnector#receiveFromNats(java.lang.String, int, java.lang.String)}.
	 * @throws Exception 
	 */
	@Test(timeout=120000, expected=IncompleteException.class)
	public void testNatsToSparkConnectorWITHOUTSubjects() throws Exception {
		NatsToSparkConnector.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY()).withNatsURL(NATS_SERVER_URL).receive();
	}
}
