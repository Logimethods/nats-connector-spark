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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.slf4j.Logger;

import com.logimethods.connector.nats_spark.IncompleteException;
import com.logimethods.connector.nats_spark.NatsSparkUtilities;

import static com.logimethods.connector.nats_spark.Constants.*;

abstract class AbstractSparkToNatsConnector<T> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected transient Integer connectionSignature;

	/**
	 * 
	 */
	protected AbstractSparkToNatsConnector() {
		super();
	}

	protected AbstractSparkToNatsConnector(String natsURL, Properties properties, Long connectionTimeout, String... subjects) {
		super();
		setProperties(properties);
		setSubjects(NatsSparkUtilities.transformIntoAList(subjects));
		setNatsURL(natsURL);
		setConnectionTimeout(connectionTimeout);
	}

	protected AbstractSparkToNatsConnector(String natsURL, Properties properties, Long connectionTimeout, Collection<String> subjects) {
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
	 * @return the storedAsKeyValue
	 */
	protected abstract boolean isStoredAsKeyValue();
	/**
	 * @param storedAsKeyValue the storedAsKeyValue to set
	 */
	protected abstract void setStoredAsKeyValue(boolean storedAsKeyValue);

	/**
	 * @param properties, the properties to set
	 * @return the connector itself
	 */
	@SuppressWarnings("unchecked")
	public T withProperties(Properties properties) {
		setProperties(properties);
		return (T)this;
	}

	/**
	 * @param subjects, the subjects to set
	 * @return the connector itself
	 */
	@SuppressWarnings("unchecked")
	public T withSubjects(String... subjects) {
		setSubjects(NatsSparkUtilities.transformIntoAList(subjects));
		return (T)this;
	}

	/**
	 * @param natsURL, the NATS URL to set
	 * @return the connector itself
	 */
	@SuppressWarnings("unchecked")
	public T withNatsURL(String natsURL) {
		setNatsURL(natsURL);
		return (T)this;
	}

	/**
	 * @param duration, the duration to set
	 * @return the connector itself
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
				if (isStoredAsKeyValue()) {
					setSubjects(new ArrayList<String>(Arrays.asList("")));
				} else {
					throw new IncompleteException("" + this + " needs at least one NATS Subject.");
				}				
			} else {
				setSubjects(NatsSparkUtilities.extractCollection(subjectsStr));
				getLogger().debug("Subject provided by the Properties: '{}'", getSubjects());				
			}
		}
		return getSubjects();
	}
	
	protected int sparkToStandardNatsConnectionSignature(String natsURL, Properties properties, Collection<String> subjects, Long connectionTimeout) {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((natsURL == null) ? 0 : natsURL.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((subjects == null) ? 0 : subjects.hashCode());
		result = prime * result + ((connectionTimeout == null) ? 0 : connectionTimeout.hashCode());
		return result;
	}
	
	protected int sparkToNatsStreamingConnectionSignature(String natsURL, Properties properties, Collection<String> subjects, Long connectionTimeout, String clusterID) {
		final int prime = 31;
		int result = 1 + sparkToStandardNatsConnectionSignature(natsURL, properties, subjects, connectionTimeout);
		result = prime * result + ((clusterID == null) ? 0 : clusterID.hashCode());
		return result;
	}

	protected abstract int computeConnectionSignature();
	
	/**
	 * @return the sealedHashCode
	 */
	protected Integer getConnectionSignature() {
		if (connectionSignature == null) {
			connectionSignature = computeConnectionSignature();
		}
		return connectionSignature;
	}

	/**
	 * @param connectionSignature the connectionSignature to set
	 */
	protected void setConnectionSignature(Integer connectionSignature) {
		this.connectionSignature = connectionSignature;
	}

}