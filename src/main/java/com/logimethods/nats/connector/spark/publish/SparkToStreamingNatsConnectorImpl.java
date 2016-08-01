/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark.publish;

import java.util.Properties;

//import io.nats.stan.Connection;
//import io.nats.stan.ConnectionFactory;
import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;

public class SparkToStreamingNatsConnectorImpl extends SparkToNatsConnector<SparkToStreamingNatsConnectorImpl> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected transient ConnectionFactory connectionFactory = null;
	protected transient Connection connection = null;

	/**
	 * 
	 */
	protected SparkToStreamingNatsConnectorImpl() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param properties
	 * @param subjects
	 */
	protected SparkToStreamingNatsConnectorImpl(Properties properties, String... subjects) {
		super(properties, subjects);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param properties
	 */
	protected SparkToStreamingNatsConnectorImpl(Properties properties) {
		super(properties);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param subjects
	 */
	protected SparkToStreamingNatsConnectorImpl(String... subjects) {
		super(subjects);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void publishToStr(String str) throws Exception {
		// TODO Auto-generated method stub
		
	}

	protected void setConnectionFactory(ConnectionFactory connectionFactory) {
		// TODO Auto-generated method stub
		
	}

	protected ConnectionFactory getConnectionFactory() {
		// TODO Auto-generated method stub
		return null;
	}

}
