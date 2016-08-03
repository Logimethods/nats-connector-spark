/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark.publish;

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
	 * @return a SparkToNatsConnector from the Pool of Connectors (if not empty), otherwise create and return a new one.
	 * @throws Exception is thrown when there is no Connection nor Subject defined.
	 */
	public SparkToNatsConnector<?> getConnector() throws Exception {
		synchronized(connectorsPool) {
			if (connectorsPool.size() > 0) {
				return connectorsPool.pollFirst();
			}
		}
		
		return new SparkToStandardNatsConnectorImpl(getNatsURL(), getDefinedProperties(), getConnectionFactory(), getDefinedSubjects());
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
}
