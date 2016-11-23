/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats_spark;

/**
 * A collection of Constant Values used by the NATS / Spark Connectors
 * 
 * @author Laurent Magnin
 */
public final class Constants {
    protected final static String PFX = "com.logimethods.connector.nats_spark.";
    
    /**
     * This property key used to specify the NATS Subjects is defined as a String {@value #PROP_SUBJECTS}
     */
    public final static String PROP_SUBJECTS = PFX + "subjects";
    
    /**
     * This property key used to specify the NATS Client ID is defined as a String {@value #PROP_CLIENT_ID}
     */
    public final static String PROP_CLIENT_ID = PFX + "client_id";
}
