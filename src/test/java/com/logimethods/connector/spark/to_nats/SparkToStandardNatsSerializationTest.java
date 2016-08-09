/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.logimethods.connector.spark.to_nats.SparkToStandardNatsConnectorImpl;

import io.nats.client.ConnectionFactory;

public class SparkToStandardNatsSerializationTest {

	private static final String natsURL = "nats://123.123.123.123:4444";
	private static final Properties properties = new Properties();
	private static final ConnectionFactory connectionFactory = new ConnectionFactory();
	private static final Collection<String> subjects = Arrays.asList("Hello", "World!");
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		properties.put("KEY", "value");
	}
	
	@Test
	public void SparkToStandardNatsConnectorImplTest() throws IOException {
		Long duration = 111l;
		SparkToStandardNatsConnectorImpl source = new SparkToStandardNatsConnectorImpl(natsURL, properties, duration, connectionFactory, subjects);
		SparkToStandardNatsConnectorImpl target = SerializationUtils.clone(source);
		assertEquals(source.getNatsURL(), target.getNatsURL());
		assertEquals(source.getProperties(), target.getProperties());
		assertEquals(source.getSubjects(), target.getSubjects());
		assertEquals(duration, target.connectionTimeout);
	}
}
