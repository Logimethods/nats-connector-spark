/**
 * 
 */
package com.logimethods.nats.connector.spark.publish;

import java.util.Collection;
import java.util.Properties;

import io.nats.client.ConnectionFactory;

/**
 * @author laugimethods
 *
 */
public class SparkToStandardNatsConnectorImpl extends SparkToNatsConnector {

	/**
	 * 
	 */
	public SparkToStandardNatsConnectorImpl() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param properties
	 * @param subjects
	 * @param connectionFactory
	 */
	public SparkToStandardNatsConnectorImpl(Properties properties, Collection<String> subjects,
			ConnectionFactory connectionFactory) {
		super(properties, subjects, connectionFactory);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param properties
	 * @param subjects
	 */
	public SparkToStandardNatsConnectorImpl(Properties properties, String... subjects) {
		super(properties, subjects);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param properties
	 */
	public SparkToStandardNatsConnectorImpl(Properties properties) {
		super(properties);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param subjects
	 */
	public SparkToStandardNatsConnectorImpl(String... subjects) {
		super(subjects);
		// TODO Auto-generated constructor stub
	}

}
