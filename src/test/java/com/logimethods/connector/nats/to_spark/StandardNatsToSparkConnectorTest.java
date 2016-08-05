/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import static com.logimethods.connector.nats.to_spark.NatsToSparkConnector.NATS_SUBJECTS;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import com.logimethods.connector.nats.spark.NatsPublisher;
import com.logimethods.connector.nats.spark.StandardNatsPublisher;
import com.logimethods.connector.nats.to_spark.NatsToSparkConnector;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnector;

import static com.logimethods.connector.nats.spark.UnitTestUtilities.NATS_SERVER_URL;

public class StandardNatsToSparkConnectorTest extends AbstractNatsToSparkTest {
	
	@Override
	protected NatsPublisher getNatsPublisher(final int nbOfMessages) {
		return new StandardNatsPublisher("np", NATS_SERVER_URL, DEFAULT_SUBJECT,  nbOfMessages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalPropertiesAndSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		properties.setProperty(SparkToNatsConnector.NATS_URL, NATS_SERVER_URL);
		final JavaReceiverInputDStream<String> messages = 
				ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).withProperties(properties).withSubjects(DEFAULT_SUBJECT));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final JavaReceiverInputDStream<String> messages = 
				ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).withNatsURL(NATS_SERVER_URL).withSubjects(DEFAULT_SUBJECT));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalSubjectsAndSystemUrl() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));
		
		System.setProperty(SparkToNatsConnector.NATS_URL, NATS_SERVER_URL);

		final JavaReceiverInputDStream<String> messages = 
				ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).withSubjects(DEFAULT_SUBJECT));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalPropertiesAndMultipleSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		final JavaReceiverInputDStream<String> messages = 
				ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY())
						.withNatsURL(NATS_SERVER_URL).withProperties(properties).withSubjects(DEFAULT_SUBJECT, "EXTRA_SUBJECT"));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalProperties() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		properties.setProperty(NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		properties.setProperty(SparkToNatsConnector.NATS_URL, NATS_SERVER_URL);
		final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).withProperties(properties));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	/**
	 * Test method for {@link com.logimethods.connector.nats.to_spark.NatsToSparkConnector#receiveFromNats(java.lang.String, int, java.lang.String)}.
	 * @throws Exception 
	 */
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWITHOUTProperties() throws Exception {
		
		try {
			NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).receive();
		} catch (Exception e) {
			if (e.getMessage().contains("NatsToSparkConnector needs at least one NATS Subject"))
				return;
			else
				throw e;
		}	

		fail("An Exception(\"NatsToSparkConnector needs at least one Subject\") should have been raised.");
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithSystemProperties() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		System.setProperty(NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		System.setProperty(SparkToNatsConnector.NATS_URL, NATS_SERVER_URL);

		try {
			final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()));

			validateTheReceptionOfMessages(ssc, messages);
		} finally {
			System.clearProperty(NATS_SUBJECTS);
		}		
	}
}
