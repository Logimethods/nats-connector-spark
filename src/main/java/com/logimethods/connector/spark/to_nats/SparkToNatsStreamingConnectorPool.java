/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

public class SparkToNatsStreamingConnectorPool extends AbstractSparkToNatsStreamingConnectorPool<SparkToNatsStreamingConnectorPool> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * 
	 */
	protected SparkToNatsStreamingConnectorPool(String clusterID) {
		super(clusterID);
	}
}
