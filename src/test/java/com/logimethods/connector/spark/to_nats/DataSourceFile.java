/**
 * 
 */
package com.logimethods.connector.spark.to_nats;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.io.Files;

/**
 * @author laugimethods
 *
 */
public class DataSourceFile extends DataSource {

	File tmpFile;
	PrintWriter writer;

	private File tempDir;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public DataSourceFile() {
		super();
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
	 * @see com.logimethods.connector.spark.to_nats.DataSource#dataStream(JavaStreamingContext ssc)
	 */
	@Override
	public JavaDStream<String> dataStream(JavaStreamingContext ssc) {
		return ssc.textFileStream(tempDir.getAbsolutePath());
	}

	@Override
	public void setup() throws IOException {
	    tempDir = Files.createTempDir();
	    tempDir.deleteOnExit();
	}

	@Override
	public void open() throws IOException {
		tmpFile = new File(tempDir.getAbsolutePath(), "tmp.txt");
		writer = new PrintWriter(tmpFile, "UTF-8");
	}

	@Override
	public void close() {
		writer.close();
	}

}
