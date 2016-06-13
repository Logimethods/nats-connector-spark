package com.logimethods.nats.connector.spark;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import org.slf4j.Logger;

public abstract class AbstractSparkToNatsConnector {
	
	public static final String NATS_SUBJECTS = "nats.io.connector.spark2nats.subjects";

	public AbstractSparkToNatsConnector() {
		super();
	}
	
	protected abstract Logger getLogger();
	
	/**
	 * @param connectionFactory the connectionFactory to set
	 */
	protected abstract void setConnectionFactory(ConnectionFactory connectionFactory);	
	protected abstract ConnectionFactory getConnectionFactory();

	/**
	 * @param connection the connection to set
	 */
//	protected abstract void setConnection(Connection connection);
//	protected abstract Connection getConnection();
	
	/**
	 * @param properties the properties to set
	 */
	protected abstract void setProperties(Properties properties);	
	protected abstract Properties getProperties();

	/**
	 * @param subjects the subjects to set
	 */
	protected abstract void setSubjects(Collection<String> subjects);	
	protected abstract Collection<String> getSubjects();

	protected Properties getDefinedProperties() {
		if (getProperties() == null) {
			setProperties(new Properties(System.getProperties()));
		}
		return getProperties();
	}

	protected Collection<String> getDefinedSubjects() throws Exception {
		if ((getSubjects() ==  null) || (getSubjects().size() == 0)) {
			final String subjectsStr = getProperties().getProperty(NATS_SUBJECTS);
			if (subjectsStr == null) {
				throw new Exception("SparkToNatsConnector needs at least one NATS Subject.");
			}
			final String[] subjectsArray = subjectsStr.split(",");
			setSubjects(Utilities.transformIntoAList(subjectsArray));
			getLogger().debug("Subject provided by the Properties: '{}'", getSubjects());
		}
		return getSubjects();
	}

	protected ConnectionFactory getDefinedConnectionFactory() throws Exception {
		if (getConnectionFactory() == null) {
			setConnectionFactory(new ConnectionFactory(getDefinedProperties()));
		}		
		return getConnectionFactory();
	}
	
	protected Connection createConnection() throws IOException, TimeoutException, Exception {
		return getDefinedConnectionFactory().createConnection();
	}
}