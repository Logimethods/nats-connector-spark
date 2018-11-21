/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark.api;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.NATS_SERVER_URL;
import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static io.nats.client.Options.PROP_URL;

import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import com.logimethods.connector.nats.spark.test.IntegerNatsPublisher;
import com.logimethods.connector.nats.spark.test.NatsPublisher;
import com.logimethods.connector.nats.to_spark.NatsToSparkConnector;

public class IntegerNatsToSparkConnectorTest extends AbstractNatsToSparkTest {
	
	@Override
	protected NatsPublisher getNatsPublisher(final int nbOfMessages) {
		return new IntegerNatsPublisher("np", NATS_SERVER_URL, DEFAULT_SUBJECT,  nbOfMessages);
	}
	
	@Test(timeout=40000)
	public void testNatsToSparkConnectorWithAdditionalPropertiesAndSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		properties.setProperty(PROP_URL, NATS_SERVER_URL);
		final JavaReceiverInputDStream<Integer> messages =  
				NatsToSparkConnector
					.receiveFromNats(Integer.class, StorageLevel.MEMORY_ONLY())
					.withProperties(properties)
					.withSubjects(DEFAULT_SUBJECT)
					.asStreamOf(ssc);

		validateTheReceptionOfIntegerMessages(ssc, messages);
	}
}