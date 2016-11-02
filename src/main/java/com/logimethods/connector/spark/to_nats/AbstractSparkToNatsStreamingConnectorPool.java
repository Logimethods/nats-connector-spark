/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import io.nats.stan.ConnectionFactory;

public abstract class AbstractSparkToNatsStreamingConnectorPool<T> extends SparkToNatsConnectorPool<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected String clusterID;
	// TODO No more static, needs to be checked on a cluster
	protected ConnectionFactory 	connectionFactory;
	
	/**
	 * 
	 */
	protected AbstractSparkToNatsStreamingConnectorPool(String clusterID) {
		super();
		this.clusterID = clusterID;
	}

	/**
	 * @return
	 * @throws Exception
	 */
	@Override
	public SparkToNatsStreamingConnectorImpl newSparkToNatsConnector() throws Exception {
		return new SparkToNatsStreamingConnectorImpl(	clusterID, 
														getNatsURL(), 
														getProperties(), 
														getConnectionTimeout(), 
														getConnectionFactory(), 
														getDefinedSubjects(),
														isStoredAsKeyValue());
	}

	/**
	 * @return the connectionFactory
	 */
	protected ConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	/**
	 * @param connectionFactory the connectionFactory to set
	 */
	protected void setConnectionFactory(ConnectionFactory factory) {
		connectionFactory = factory;
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
		return "SparkToNatsStreamingConnectorPool ["
				+ (connectionFactory != null ? "connectionFactory=" + connectionFactory + ", " : "")
				+ (properties != null ? "properties=" + properties + ", " : "")
				+ (subjects != null ? "subjects=" + subjects + ", " : "")
				+ (natsURL != null ? "natsURL=" + natsURL + ", " : "")
				+ ("connectorsPoolMap=" + connectorsPoolMap) + "]";
	}
}
