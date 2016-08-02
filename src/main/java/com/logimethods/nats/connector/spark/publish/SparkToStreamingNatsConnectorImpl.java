/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark.publish;

import java.util.Collection;
import java.util.Properties;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

public class SparkToStreamingNatsConnectorImpl extends SparkToNatsConnector<SparkToStreamingNatsConnectorImpl> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected transient ConnectionFactory connectionFactory = null;
	protected transient Connection connection = null;

	/**
	 * @param properties
	 * @param connectionFactory
	 * @param subjects
	 */
	protected SparkToStreamingNatsConnectorImpl(Properties properties, ConnectionFactory connectionFactory, Collection<String> subjects) {
		super(properties, subjects);
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @param properties
	 * @param connectionFactory
	 * @param subjects
	 */
	protected SparkToStreamingNatsConnectorImpl(Properties properties, ConnectionFactory connectionFactory, String... subjects) {
		super(properties, subjects);
		this.connectionFactory = connectionFactory;
	}

	@Override
	protected void publishToStr(String str) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
