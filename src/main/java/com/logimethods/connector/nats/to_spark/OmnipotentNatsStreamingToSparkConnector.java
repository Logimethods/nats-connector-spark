/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.streaming.Connection;
import io.nats.streaming.ConnectionFactory;
import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;

/**
 * A NATS to Spark Connector.
 * <p>
 * It will transfer messages received from NATS into Spark data.
 * <p>
 * That class extends {@link com.logimethods.connector.nats.to_spark.NatsToSparkConnector}&lt;T,R,V&gt;.
 */
public abstract class OmnipotentNatsStreamingToSparkConnector<T,R,V> extends NatsToSparkConnector<T,R,V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static final Logger logger = LoggerFactory.getLogger(OmnipotentNatsStreamingToSparkConnector.class);

	protected String clusterID, clientID;
	protected SubscriptionOptions opts;
	protected SubscriptionOptions.Builder optsBuilder;

	/* Constructors with subjects provided by the environment */
	
	protected OmnipotentNatsStreamingToSparkConnector(Class<V> type, StorageLevel storageLevel, String clusterID, String clientID) {
		super(type, storageLevel);
		this.clusterID = clusterID;
		this.clientID = clientID;
		setQueue();
//		logger.debug("CREATE NatsToSparkConnector {} with Properties '{}', Storage Level {} and NATS Subjects '{}'.", this, properties, storageLevel, subjects);
	}

	/**
	 * @param optsBuilder, the NATS Streaming options used to set the connection to NATS
	 * @return a NATS Streaming to Spark Connector
	 */
	public OmnipotentNatsStreamingToSparkConnector<T,R,V> withSubscriptionOptionsBuilder(SubscriptionOptions.Builder optsBuilder) {
		this.optsBuilder = optsBuilder;
		return this;
	}

    /**
     * Sets the durable subscriber name for the subscription.
     * 
     * @param durableName the name of the durable subscriber
     * @return the connector itself
     */
    public OmnipotentNatsStreamingToSparkConnector<T,R,V> setDurableName(String durableName) {
    	getOptsBuilder().setDurableName(durableName);
    	return this;
    }

    /**
     * Sets the maximum number of in-flight (unacknowledged) messages for the subscription.
     * 
     * @param maxInFlight the maximum number of in-flight messages
     * @return the connector itself
     */
    public OmnipotentNatsStreamingToSparkConnector<T,R,V> setMaxInFlight(int maxInFlight) {
    	getOptsBuilder().setMaxInFlight(maxInFlight);
        return this;
    }

    /**
     * Sets the amount of time the subscription will wait for ACKs from the cluster.
     * 
     * @param ackWait the amount of time the subscription will wait for an ACK from the cluster
     * @return the connector itself
     */
    public OmnipotentNatsStreamingToSparkConnector<T,R,V> setAckWait(Duration ackWait) {
    	getOptsBuilder().setAckWait(ackWait);
        return this;
    }

    /**
     * Sets the amount of time the subscription will wait for ACKs from the cluster.
     * 
     * @param ackWait the amount of time the subscription will wait for an ACK from the cluster
     * @param unit the time unit
     * @return the connector itself
     */
    public OmnipotentNatsStreamingToSparkConnector<T,R,V> setAckWait(long ackWait, TimeUnit unit) {
    	getOptsBuilder().setAckWait(ackWait, unit);
        return this;
    }

    /**
     * Sets whether or not messages must be acknowledge individually by calling
     * {@link Message#ack()}.
     * 
     * @param manualAcks whether or not messages must be manually acknowledged
     * @return the connector itself
     */
    public OmnipotentNatsStreamingToSparkConnector<T,R,V> setManualAcks(boolean manualAcks) {
    	getOptsBuilder().setManualAcks(manualAcks);
        return this;
    }

    /**
     * Specifies the sequence number from which to start receiving messages.
     * 
     * @param seq the sequence number from which to start receiving messages
     * @return the connector itself
     */
    public OmnipotentNatsStreamingToSparkConnector<T,R,V> startAtSequence(long seq) {
    	getOptsBuilder().startAtSequence(seq);
        return this;
    }

    /**
     * Specifies the desired start time position using {@code java.time.Instant}.
     * 
     * @param start the desired start time position expressed as a {@code java.time.Instant}
     * @return the connector itself
     */
    public OmnipotentNatsStreamingToSparkConnector<T,R,V> startAtTime(Instant start) {
    	getOptsBuilder().startAtTime(start);
        return this;
    }

    /**
     * Specifies the desired delta start time position in the desired unit.
     * 
     * @param ago the historical time delta (from now) from which to start receiving messages
     * @param unit the time unit
     * @return the connector itself
     */
    public OmnipotentNatsStreamingToSparkConnector<T,R,V> startAtTimeDelta(long ago, TimeUnit unit) {
    	getOptsBuilder().startAtTimeDelta(ago, unit);
        return this;
    }

    /**
     * Specifies the desired delta start time as a {@link java.time.Duration}.
     * 
     * @param ago the historical time delta (from now) from which to start receiving messages
     * @return the connector itself
     */
    public OmnipotentNatsStreamingToSparkConnector<T,R,V> startAtTimeDelta(Duration ago) {
    	getOptsBuilder().startAtTimeDelta(ago);
        return this;
    }

    /**
     * Specifies that message delivery should start with the last (most recent) message stored
     * for this subject.
     * 
     * @return the connector itself
     */
    public OmnipotentNatsStreamingToSparkConnector<T,R,V> startWithLastReceived() {
    	getOptsBuilder().startWithLastReceived();
        return this;
    }

    /**
     * Specifies that message delivery should begin at the oldest available message for this
     * subject.
     * 
     * @return the connector itself
     */
    public OmnipotentNatsStreamingToSparkConnector<T,R,V> deliverAllAvailable() {
    	getOptsBuilder().deliverAllAvailable();
        return this;
    }

	
	/**
	 * @return the opts
	 */
	protected SubscriptionOptions getSubscriptionOptions() {
		if ((opts == null) && (optsBuilder != null)){
			opts = optsBuilder.build();
		}
		return opts;
	}

	/**
	 * @return the optsBuilder
	 */
	protected SubscriptionOptions.Builder getOptsBuilder() {
		if (optsBuilder == null) {
			optsBuilder = new SubscriptionOptions.Builder();
		}
		return optsBuilder;
	}

	/**
	 * @return a NATS Streaming to Spark Connector where the NATS Messages are stored in Spark as Key (the NATS Subject) / Value (the NATS Payload)
	 */
	public NatsStreamingToKeyValueSparkConnectorImpl<V> storedAsKeyValue() {
		return new NatsStreamingToKeyValueSparkConnectorImpl<V>(type, storageLevel(), subjects, properties, queue, natsUrl, clusterID, clientID, 
																opts, optsBuilder, dataDecoder, scalaDataDecoder);
	}

	/** Create a socket connection and receive data until receiver is stopped 
	 * @throws Exception **/
	protected void receive() throws Exception {

		// Make connection and initialize streams			  
		final ConnectionFactory connectionFactory = new ConnectionFactory(clusterID, clientID);
		if (getNatsUrl() != null) {
			connectionFactory.setNatsUrl(getNatsUrl());
		}
		final Connection connection = connectionFactory.createConnection();
//		logger.info("A NATS from '{}' to Spark Connection has been created for '{}', sharing Queue '{}'.", connection.getConnectedUrl(), this, queue);
		
		for (String subject: getSubjects()) {
			final Subscription sub = connection.subscribe(subject, queue, getMessageHandler(), getSubscriptionOptions());
			logger.info("Listening on {}.", subject);
			
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
				@Override
				public void run() {
					logger.debug("Caught CTRL-C, shutting down gracefully..." + this);
					try {
						sub.unsubscribe();
					} catch (IOException | TimeoutException e) {
						if (logger.isDebugEnabled()) {
							logger.error("Exception while unsubscribing " + e.toString());
						}
					}
					try {
						connection.close();
					} catch (IOException | TimeoutException e) {
						if (logger.isDebugEnabled()) {
							logger.error("Exception while unsubscribing " + e.toString());
						}
					}
				}
			}));
		}
	}

	abstract protected MessageHandler getMessageHandler();
}

