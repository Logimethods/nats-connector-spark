/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import static io.nats.client.Constants.PROP_URL;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats_spark.Utilities;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

public class SparkToNatsStreamingConnectorImpl extends SparkToNatsConnector<SparkToNatsStreamingConnectorImpl> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected static final Logger logger = LoggerFactory.getLogger(SparkToNatsStreamingConnectorImpl.class);
	protected final String clusterID;
	protected final static String CLIENT_ID_ROOT = "SparkToNatsStreamingConnector_";
	protected transient String clientID;
	protected transient ConnectionFactory connectionFactory;
	protected transient Connection connection;

	/**
	 * 
	protected SparkToNatsStreamingConnectorImpl(String clusterID) {
		super();
		this.clusterID = clusterID;
	}
	 */

	/**
	 * @param properties
	 * @param connectionFactory
	 * @param subjects
	 */
	protected SparkToNatsStreamingConnectorImpl(String clusterID, String natsURL, Properties properties, Long connectionTimeout, ConnectionFactory connectionFactory, Collection<String> subjects) {
		super(natsURL, properties, connectionTimeout, subjects);
		this.connectionFactory = connectionFactory;
		this.clusterID = clusterID;
	}

	/**
	 * @param properties
	 * @param connectionFactory
	 * @param subjects
	 */
	protected SparkToNatsStreamingConnectorImpl(String clusterID, String natsURL, Properties properties, Long connectionTimeout, ConnectionFactory connectionFactory, String... subjects) {
		super(natsURL, properties, connectionTimeout, subjects);
		this.connectionFactory = connectionFactory;
		this.clusterID = clusterID;
	}

	/**
	 * @return the clientID
	 */
	protected String getClientID() {
		if (clientID == null ) {
			clientID = CLIENT_ID_ROOT + Utilities.generateUniqueID(this);
		}
		return clientID;
	}

	/**
	 * A method that will publish the provided String into NATS through the defined subjects.
	 * @param obj the String that will be published to NATS.
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	@Override
	protected void publishToStr(String str) throws Exception {
		resetClosingTimeout();
		
		logger.debug("Received '{}' from Spark", str);
		
		final byte[] payload = str.getBytes();
		final Connection localConnection = getConnection();
		for (String subject : getDefinedSubjects()) {
			localConnection.publish(subject, payload);
	
			logger.trace("Publish '{}' from Spark to NATS STREAMING ({})", str, subject);
		}
	}

	// TODO Check Javadoc
	/**
	 * A method that will publish the provided String into NATS through the defined subjects.
	 * @param obj the String that will be published to NATS.
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	@Override
	protected void publishToStr(String postSubject, String message) throws Exception {
		resetClosingTimeout();
		
		logger.debug("Received '{}' from Spark with '{}' Subject", message, postSubject);
		
		final byte[] payload = message.getBytes();
		final Connection localConnection = getConnection();
		for (String preSubject : getDefinedSubjects()) {
			final String subject = preSubject + postSubject;
			localConnection.publish(subject, payload);
	
			logger.trace("Publish '{}' from Spark to NATS STREAMING ({})", message, subject);
		}
	}

	protected synchronized Connection getConnection() throws Exception {
		if (connection == null) {
			connection = createConnection();
		}
		return connection;
	}

	protected ConnectionFactory getConnectionFactory() throws Exception {
		if (connectionFactory == null) {
			connectionFactory = new ConnectionFactory(clusterID, getClientID());
			connectionFactory.setNatsUrl(getNatsURL());
		}		
		return connectionFactory;
	}
	
	protected Connection createConnection() throws IOException, TimeoutException, Exception {
		final Connection newConnection = getConnectionFactory().createConnection();
		logger.debug("A NATS Connection {} has been created for {}", newConnection, this);
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
			@Override
			public void run() {
				logger.debug("Caught CTRL-C, shutting down gracefully..." + this);
				try {
					newConnection.close();
				} catch (IOException | TimeoutException e) {
					if (logger.isDebugEnabled()) {
						logger.error("Exception while unsubscribing " + e.toString());
					}
				}
			}
		}));
		return newConnection;
	}

	@Override
	protected synchronized void closeConnection() {
		logger.debug("At {}, ready to close '{}' by {}", new Date().getTime(), connection, super.toString());
		removeFromPool();

		if (connection != null) {
			try {
				connection.close();
				logger.debug("{} has been CLOSED by {}", connection, super.toString());
			} catch (IOException | TimeoutException e) {
				if (logger.isDebugEnabled()) {
					logger.error("Exception while closing the connection: {} by {}", e, this);
				}
			}
			connection = null;
		}
	}
	
	@Override
	protected void removeFromPool() {
		SparkToNatsStreamingConnectorPool.removeConnectorFromPool(this);
	}
	
	protected String getsNatsUrlKey() {
		return PROP_URL;
	}

	@Override
	public int computeConnectionSignature() {
		return sparkToNatsStreamingConnectionSignature(natsURL, properties, subjects, connectionTimeout, clusterID);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SparkToNatsStreamingConnectorImpl [" + Integer.toHexString(hashCode()) + " : "
				+ (clusterID != null ? " : clusterID=" + clusterID + ", " : "")
				+ (clientID != null ? "clientID=" + clientID + ", " : "")
				+ (connectionFactory != null ? "connectionFactory=" + connectionFactory + ", " : "")
				+ (connection != null ? "connection=" + connection + ", " : "")
				+ (properties != null ? "properties=" + properties + ", " : "")
				+ (subjects != null ? "subjects=" + subjects + ", " : "")
				+ (natsURL != null ? "natsURL=" + natsURL + ", " : "")
				+ (publishToNats != null ? "publishToNats=" + publishToNats : "") + "]";
	}
}
