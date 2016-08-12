/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats_spark.Utilities;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

import static io.nats.client.Constants.*;

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
			clientID = CLIENT_ID_ROOT + Utilities.generateUniqueID();
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

	protected synchronized Connection getConnection() throws Exception {
		if (connection == null) {
			connection = createConnection();
			logger.debug("A NATS Connection {} has been created for {}", connection, this);
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
		logger.debug("Ready to close '{}' by {}", connection, super.toString());
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
	protected boolean hasANotNullConnection() {
		return connection != null;
	}

	protected String getsNatsUrlKey() {
		return PROP_URL;
	}

	@Override
	protected Properties getEnrichedProperties() {
		// TODO Inverse : from properties to url...
/*		if ((getProperties() != null) && (enrichedProperties == null)) {
			enrichedProperties = getProperties();
		}*/
		return properties;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((clientID == null) ? 0 : clientID.hashCode());
		result = prime * result + ((clusterID == null) ? 0 : clusterID.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof SparkToNatsStreamingConnectorImpl))
			return false;
		SparkToNatsStreamingConnectorImpl other = (SparkToNatsStreamingConnectorImpl) obj;
		if (clientID == null) {
			if (other.clientID != null)
				return false;
		} else if (!clientID.equals(other.clientID))
			return false;
		if (clusterID == null) {
			if (other.clusterID != null)
				return false;
		} else if (!clusterID.equals(other.clusterID))
			return false;
		return true;
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
