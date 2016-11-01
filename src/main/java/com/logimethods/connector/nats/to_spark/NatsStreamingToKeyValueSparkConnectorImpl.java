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

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.stan.Message;
import io.nats.stan.MessageHandler;
import io.nats.stan.SubscriptionOptions;
import scala.Tuple2;

/**
 * A NATS to Spark Connector.
 * <p>
 * It will transfer messages received from NATS into Spark data.
 * <p>
 * That class extends {@link org.apache.spark.streaming.receiver.Receiver}&lt;String&gt;.
 * <p>
 * An usage of this class would look like this.
 * <pre>
 * JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
 * final JavaReceiverInputDStream&lt;String&gt; messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY(), DEFAULT_SUBJECT));
 * </pre>
 * @see <a href="http://spark.apache.org/docs/1.6.2/streaming-custom-receivers.html">Spark Streaming Custom Receivers</a>
 */
public class NatsStreamingToKeyValueSparkConnectorImpl 
				extends OmnipotentNatsStreamingToSparkConnector<NatsStreamingToKeyValueSparkConnectorImpl, Tuple2<String, String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static final Logger logger = LoggerFactory.getLogger(NatsStreamingToKeyValueSparkConnectorImpl.class);

	/* Constructors with subjects provided by the environment */
	
	protected NatsStreamingToKeyValueSparkConnectorImpl(StorageLevel storageLevel, String clusterID, String clientID) {
		super(storageLevel, clusterID, clientID);
	}

	public NatsStreamingToKeyValueSparkConnectorImpl(StorageLevel storageLevel, Collection<String> subjects,
			Properties properties, String queue, String natsUrl, String clusterID, String clientID, 
			SubscriptionOptions opts, SubscriptionOptions.Builder optsBuilder) {
		super(storageLevel, clusterID, clientID);
		this.subjects = subjects;
		this.properties = properties;
		this.queue = queue;
		this.natsUrl = natsUrl;
		this.opts = opts;
		this.optsBuilder = optsBuilder;
	}
	
	/**
	@SuppressWarnings("unchecked")
	*/
	public JavaPairDStream<String, String> asStreamOf(JavaStreamingContext ssc) {
		return ssc.receiverStream(this).mapToPair(keepTuple2Func);
	}
	
	/**
	@SuppressWarnings("unchecked")
	*/
	public ReceiverInputDStream<Tuple2<String, String>> asStreamOf(StreamingContext ssc) {
		return ssc.receiverStream(this, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class));
	}

	@Override
	protected MessageHandler getMessageHandler() {
		return new MessageHandler() {
			@Override
			public void onMessage(Message m) {
				final String subject = m.getSubject();
				final String s = new String(m.getData());

				if (logger.isTraceEnabled()) {
					logger.trace("Received by {} on Subject '{}': {}.", NatsStreamingToKeyValueSparkConnectorImpl.this,
							m.getSubject(), s);
				}
				store(new Tuple2<String, String>(subject, s));
			}
		};
	}
}

