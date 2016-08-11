/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import java.util.HashMap;
import java.util.LinkedList;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

public class SparkToNatsStreamingConnectorPool extends SparkToNatsConnectorPool<SparkToNatsStreamingConnectorPool> {

	protected static final HashMap<Integer, LinkedList<Connection>> connectionsPoolMap = new HashMap<Integer, LinkedList<Connection>>();

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
	protected SparkToNatsStreamingConnectorPool(String clusterID) {
		super();
		this.clusterID = clusterID;
	}

	/**
	 * @return
	 * @throws Exception
	 */
	@Override
	public SparkToNatsStreamingConnectorImpl newSparkToNatsConnector() throws Exception {
		return new SparkToNatsStreamingConnectorImpl(clusterID, getNatsURL(), getProperties(), getConnectionTimeout(), getConnectionFactory(), getDefinedSubjects());
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
	protected void returnConnection(int hashCode, SparkToNatsConnector<?> connector) {
		synchronized(connectionsPoolMap) {
			LinkedList<Connection> connectorsPoolList = connectionsPoolMap.get(hashCode);
			if (connectorsPoolList == null) {
				connectorsPoolList = new LinkedList<Connection>();
				connectionsPoolMap.put(hashCode, connectorsPoolList);
			}
			connectorsPoolList.add(((SparkToNatsStreamingConnectorImpl)connector).connection);
		}
	}

	public static long poolSize() {
		int size = 0;
		for (LinkedList<Connection> poolList: connectionsPoolMap.values()){
			size += poolList.size();
		}
		return size;
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
				+ ("connectionsPoolMap=" + connectionsPoolMap) + "]";
	}
}
