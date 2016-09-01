/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.spark.to_nats;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;

import org.slf4j.Logger;

import com.logimethods.connector.nats_spark.IncompleteException;
import com.logimethods.connector.nats_spark.Utilities;

import static com.logimethods.connector.nats_spark.Constants.*;

@SuppressWarnings("serial")
public abstract class AbstractSparkToNatsConnector<T> implements Serializable {

	protected transient Integer sealedHashCode;

	/**
	 * 
	 */
	protected AbstractSparkToNatsConnector() {
		super();
	}

	public AbstractSparkToNatsConnector(String natsURL, Properties properties, Long connectionTimeout, String... subjects) {
		super();
		setProperties(properties);
		setSubjects(Utilities.transformIntoAList(subjects));
		setNatsURL(natsURL);
		setConnectionTimeout(connectionTimeout);
	}

	public AbstractSparkToNatsConnector(String natsURL, Properties properties, Long connectionTimeout, Collection<String> subjects) {
		super();
		setProperties(properties);
		setSubjects(subjects);
		setNatsURL(natsURL);		
		setConnectionTimeout(connectionTimeout);
	}
	
	protected abstract Logger getLogger();
	
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

	/**
	 * @param subjects the subjects to set
	 */
	protected abstract void setNatsURL(String natsURL);	
	protected abstract String getNatsURL();

	/**
	 * @return the connectionTimeout
	 */
	protected abstract Long getConnectionTimeout();
	/**
	 * @param connectionTimeout the connectionTimeout to set
	 */
	protected abstract void setConnectionTimeout(Long connectionTimeout);

	/**
	 * @param properties the properties to set
	 */
	@SuppressWarnings("unchecked")
	public T withProperties(Properties properties) {
		setProperties(properties);
		return (T)this;
	}

	/**
	 * @param subjects the subjects to set
	 */
	@SuppressWarnings("unchecked")
	public T withSubjects(String... subjects) {
		setSubjects(Utilities.transformIntoAList(subjects));
		return (T)this;
	}

	/**
	 * @param natsURL the NATS URL to set
	 */
	@SuppressWarnings("unchecked")
	public T withNatsURL(String natsURL) {
		setNatsURL(natsURL);
		return (T)this;
	}

	/**
	 * @param connectionTimeout the connectionTimeout to set
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public T withConnectionTimeout(Duration duration) {
		setConnectionTimeout(duration.toNanos());
		return (T)this;
	}

	protected Collection<String> getDefinedSubjects() throws IncompleteException {
		if ((getSubjects() ==  null) || (getSubjects().size() == 0)) {
			final String subjectsStr = getProperties() != null ? 
											getProperties().getProperty(PROP_SUBJECTS) : 
											null;
			if (subjectsStr == null) {
				throw new IncompleteException("" + this + " needs at least one NATS Subject.");
			}
			setSubjects(Utilities.extractCollection(subjectsStr));
			getLogger().debug("Subject provided by the Properties: '{}'", getSubjects());
		}
		return getSubjects();
	}
	
	public int sparkToStandardNatsConnectionSignature(String natsURL, Properties properties, Collection<String> subjects, Long connectionTimeout) {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((natsURL == null) ? 0 : natsURL.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((subjects == null) ? 0 : subjects.hashCode());
		result = prime * result + ((connectionTimeout == null) ? 0 : connectionTimeout.hashCode());
		return result;
	}
	
	public int sparkToNatsStreamingConnectionSignature(String natsURL, Properties properties, Collection<String> subjects, Long connectionTimeout, String clientID, String clusterID) {
		final int prime = 31;
		int result = sparkToStandardNatsConnectionSignature(natsURL, properties, subjects, connectionTimeout);
		result = prime * result + ((clientID == null) ? 0 : clientID.hashCode());
		result = prime * result + ((clusterID == null) ? 0 : clusterID.hashCode());
		return result;
	}

	protected abstract int getConnectionSignature();
	
	/**
	 * @return the sealedHashCode
	 */
	protected Integer sealedHashCode() {
		if (sealedHashCode == null) {
			sealedHashCode = hashCode();
		}
		return sealedHashCode;
	}

}