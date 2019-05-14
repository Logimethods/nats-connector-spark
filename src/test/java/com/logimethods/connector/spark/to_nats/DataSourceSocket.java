/**
 * 
 */
package com.logimethods.connector.spark.to_nats;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author laugimethods
 *
 */
public class DataSourceSocket extends DataSource {
	
	protected static Logger logger = LoggerFactory.getLogger(DataSourceSocket.class);
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	PrintWriter writer;
	private String hostnameWrite, hostnameRead;
	private int portWrite;
	private int portRead;
	private Socket socket;

	/**
	 * @param hostname
	 * @param port
	 */
	public DataSourceSocket(String hostnameWrite, int portWrite, String hostnameRead, int portRead) {
		super();
		this.hostnameWrite = hostnameWrite;
		this.portWrite = portWrite;
		this.hostnameRead = hostnameRead;
		this.portRead = portRead;
	}

	@Override
	public void write(String str) {
		writer.println(str);
	}

	@Override
	public void write(List<?> data) {
		for(Object str: data) {
			writer.println(str.toString());
		}
		writer.flush();
	}

	/* (non-Javadoc)
	 * @see com.logimethods.connector.spark.to_nats.DataSource#dataStream(org.apache.spark.streaming.api.java.JavaStreamingContext)
	 */
	@Override
	public JavaDStream<String> dataStream(JavaStreamingContext ssc) {
		return ssc.socketTextStream(hostnameRead, portRead);
	}

	/* (non-Javadoc)
	 * @see com.logimethods.connector.spark.to_nats.DataSource#setup()
	 */
	@Override
	public void setup() throws IOException {
		try {
			logger.info("new Socket({}, {}) TENTATIVE", hostnameWrite, portWrite);
			socket = new Socket(hostnameWrite, portWrite);
		} catch (IOException e) {
			logger.error("new Socket({}, {}) PRODUCES {}", hostnameWrite, portWrite, e.getMessage());
			throw(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.logimethods.connector.spark.to_nats.DataSource#open()
	 */
	@Override
	public void open() throws IOException {
		writer = new PrintWriter( new OutputStreamWriter(socket.getOutputStream()));
	}

	/* (non-Javadoc)
	 * @see com.logimethods.connector.spark.to_nats.DataSource#close()
	 */
	@Override
	public void close() throws IOException {
		writer.close();
		socket.close();
	}

}
