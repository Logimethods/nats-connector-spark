/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import scala.Tuple2;

/**
 * A NATS Streaming to Spark Connector.
 * <p>
 * It will transfer messages received from NATS into Spark data.
 * <p>
 * That class extends {@link com.logimethods.connector.nats.to_spark.NatsToSparkConnector}&lt;T,R,V&gt;.
 */
public class NatsStreamingToSparkConnectorImpl<R> extends OmnipotentNatsStreamingToSparkConnector<NatsStreamingToSparkConnectorImpl<R>, R, R> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static final Logger logger = LoggerFactory.getLogger(NatsStreamingToSparkConnectorImpl.class);

	/* Constructors with subjects provided by the environment */
	
	protected NatsStreamingToSparkConnectorImpl(Class<R> type, StorageLevel storageLevel, String clusterID, String clientID) {
		super(type, storageLevel, clusterID, clientID);
	}
	
	/**
	 * @param ssc, the (Java based) Spark Streaming Context
	 * @return a Spark Stream, belonging to the provided Context, that will collect NATS Messages
	 */
	public JavaReceiverInputDStream<R> asStreamOf(JavaStreamingContext ssc) {
		return ssc.receiverStream(this);
	}
	
	/**
	 * @param ssc, the (Scala based) Spark Streaming Context
	 * @return a Spark Stream, belonging to the provided Context, that will collect NATS Messages
	 */
	public ReceiverInputDStream<R> asStreamOf(StreamingContext ssc) {
		return ssc.receiverStream(this, scala.reflect.ClassTag$.MODULE$.apply(String.class));
	}
	
	/**
	 * @param ssc, the (Java based) Spark Streaming Context
	 * @return a Spark Stream, belonging to the provided Context, 
	 * that will collect NATS Messages as Key (the NATS Subject) / Value (the NATS Payload)
	 */
	public JavaPairDStream<String, R> asStreamOfKeyValue(JavaStreamingContext ssc) {
		return ssc.receiverStream(this.storedAsKeyValue()).mapToPair(tuple -> tuple);
	}
	
	/**
	 * @param ssc, the (Scala based) Spark Streaming Context
	 * @return a Spark Stream, belonging to the provided Context, 
	 * that will collect NATS Messages as Tuples of (the NATS Subject) / (the NATS Payload)
	 */
	public ReceiverInputDStream<Tuple2<String, R>> asStreamOfKeyValue(StreamingContext ssc) {
		return ssc.receiverStream(this.storedAsKeyValue(), scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class));
	}

	@Override
	protected MessageHandler getMessageHandler() {
		return new MessageHandler() {
			@Override
			public void onMessage(Message m) {
				R s = decodeData(m);
				if (logger.isTraceEnabled()) {
					logger.trace("Received by {} on Subject '{}': {}.", NatsStreamingToSparkConnectorImpl.this,
							m.getSubject(), s);
				}
				store(s);
			}
		};
	}
}

