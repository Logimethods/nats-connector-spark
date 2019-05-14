/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats.api;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.NATS_URL;

import java.time.Duration;

import org.apache.spark.streaming.api.java.JavaDStream;

import com.logimethods.connector.spark.to_nats.AbstractSparkToStandardNatsConnectorLifecycleTest;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnectorPool;

@SuppressWarnings("serial")
public class SparkToStandardNatsConnectorLifecycleTest extends AbstractSparkToStandardNatsConnectorLifecycleTest {

	protected void publishToNats(final String subject1, final String subject2, final int partitionsNb) {
		final JavaDStream<String> lines = dataSource.dataStream(ssc).repartition(partitionsNb);
		
		SparkToNatsConnectorPool
			.newPool()
			.withNatsURL(NATS_URL)
			.withConnectionTimeout(Duration.ofSeconds(2))
			.withSubjects(DEFAULT_SUBJECT, subject1, subject2)
			.publishToNats(lines);
	}
}
