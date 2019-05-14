/**
 * 
 */
package com.logimethods.connector.spark.to_nats;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author laugimethods
 *
 */
public abstract class DataSource implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public DataSource() {
	}
	
	public abstract void write(String str);

	public abstract void write(List<?> data);
	
	public abstract JavaDStream<String> dataStream(JavaStreamingContext ssc);

	public abstract void setup() throws IOException;
	
	public abstract void open() throws IOException;
	
	public abstract void close() throws IOException;

}
