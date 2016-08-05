/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import io.nats.client.ConnectionFactory;

public class SparkToStandardNatsConnectorPool extends SparkToNatsConnectorPool<SparkToStandardNatsConnectorPool> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// TODO No more static, needs to be checked on a cluster
	protected ConnectionFactory 	connectionFactory;
	
	/**
	 * 
	 */
	protected SparkToStandardNatsConnectorPool() {
		super();
	}

	/**
	 * @return
	 * @throws Exception
	 */
	protected SparkToStandardNatsConnectorImpl newSparkToNatsConnector() throws Exception {
		return new SparkToStandardNatsConnectorImpl(getNatsURL(), getProperties(), getConnectionFactory(), getDefinedSubjects());
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

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SparkToStandardNatsConnectorPool ["
				+ (connectionFactory != null ? "connectionFactory=" + connectionFactory + ", " : "")
				+ (properties != null ? "properties=" + properties + ", " : "")
				+ (subjects != null ? "subjects=" + subjects + ", " : "")
				+ (natsURL != null ? "natsURL=" + natsURL + ", " : "")
				+ ("connectorsPoolMap=" + connectorsPoolMap) + "]";
	}
}
