/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import io.nats.streaming.Options;

public abstract class AbstractSparkToNatsStreamingConnectorPool<T> extends SparkToNatsConnectorPool<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected String clusterID;
	// TODO No more static, needs to be checked on a cluster
	protected Options.Builder optionsBuilder;
	
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
														getOptionsBuilder(), 
														getDefinedSubjects(),
														isStoredAsKeyValue());
	}

	/**
	 * @return the optionsBuilder
	 */
	protected Options.Builder getOptionsBuilder() {
		return optionsBuilder;
	}

	/**
	 * @param optionsBuilder the optionsBuilder to set
	 */
	protected void setOptionsBuilder(Options.Builder optionsBuilder) {
		this.optionsBuilder = optionsBuilder;
	}

	@Override
	protected int computeConnectionSignature() {
		return sparkToNatsStreamingConnectionSignature(natsURL, properties, subjects, connectionTimeout, clusterID);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SparkToNatsStreamingConnectorPool ["
				+ (optionsBuilder != null ? "optionsBuilder=" + optionsBuilder + ", " : "")
				+ (properties != null ? "properties=" + properties + ", " : "")
				+ (subjects != null ? "subjects=" + subjects + ", " : "")
				+ (natsURL != null ? "natsURL=" + natsURL + ", " : "")
				+ ("connectorsPoolMap=" + connectorsPoolMap) + "]";
	}
}
