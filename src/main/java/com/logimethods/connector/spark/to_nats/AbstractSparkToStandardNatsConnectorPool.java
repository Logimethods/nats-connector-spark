/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

public abstract class AbstractSparkToStandardNatsConnectorPool<T> extends SparkToNatsConnectorPool<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * 
	 */
	protected AbstractSparkToStandardNatsConnectorPool() {
		super();
	}

	/**
	 * @return
	 * @throws Exception
	 */
	protected SparkToStandardNatsConnectorImpl newSparkToNatsConnector() throws Exception {
		return new SparkToStandardNatsConnectorImpl(	getNatsURL(), 
														getProperties(), 
														getConnectionTimeout(), 
														getDefinedSubjects(),
														isStoredAsKeyValue());
	}

	@Override
	protected int computeConnectionSignature() {
		return sparkToStandardNatsConnectionSignature(natsURL, properties, subjects, connectionTimeout);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SparkToStandardNatsConnectorPool ["
				+ (properties != null ? "properties=" + properties + ", " : "")
				+ (subjects != null ? "subjects=" + subjects + ", " : "")
				+ (natsURL != null ? "natsURL=" + natsURL + ", " : "") + "]";
	}
}
