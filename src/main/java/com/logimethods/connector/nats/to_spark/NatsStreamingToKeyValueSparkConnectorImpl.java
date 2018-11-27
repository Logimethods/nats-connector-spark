/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import java.util.Collection;
import java.util.Properties;
import java.util.function.Function;

import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.SubscriptionOptions;
import scala.Tuple2;

/**
 * A NATS Streaming to a Key/Value Spark Stream Connector.
 * <p>
 * It will transfer messages received from NATS into Spark data.
 * <p>
 * That class extends {@link com.logimethods.connector.nats.to_spark.NatsToSparkConnector}&lt;T,R,V&gt;.
 */
public class NatsStreamingToKeyValueSparkConnectorImpl<V> 
				extends OmnipotentNatsStreamingToSparkConnector<NatsStreamingToKeyValueSparkConnectorImpl<V>, Tuple2<String, V>, V> {

	private static final long serialVersionUID = 1L;

	protected static final Logger logger = LoggerFactory.getLogger(NatsStreamingToKeyValueSparkConnectorImpl.class);

	protected NatsStreamingToKeyValueSparkConnectorImpl(Class<V> type, StorageLevel storageLevel, Collection<String> subjects,
			Properties properties, String queue, String natsUrl, String clusterID, String clientID, 
			SubscriptionOptions subscriptionOpts, SubscriptionOptions.Builder subscriptionOptsBuilder, Function<byte[], V> dataDecoder, scala.Function1<byte[], V> scalaDataDecoder) {
		super(type, storageLevel, clusterID, clientID);
		this.subjects = subjects;
		this.properties = properties;
		this.queue = queue;
		this.natsUrl = natsUrl;
		this.subscriptionOpts = subscriptionOpts;
		this.subscriptionOptsBuilder = subscriptionOptsBuilder;
		this.dataDecoder = dataDecoder;
		this.scalaDataDecoder = scalaDataDecoder;
	}

	@Override
	protected MessageHandler getMessageHandler() {
		return new MessageHandler() {
			@Override
			public void onMessage(Message m) {
				final Tuple2<String, V> s = decodeTuple(m);

				if (logger.isTraceEnabled()) {
					logger.trace("Received by {} on Subject '{}': {}.", NatsStreamingToKeyValueSparkConnectorImpl.this,
							m.getSubject(), s);
				}
				
				store(s);
			}
		};
	}
}

