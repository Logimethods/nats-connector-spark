/*******************************************************************************
 * Copyright (c) 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.spark;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class NATSServer implements AutoCloseable
{
	final static String GNATSD = "gnatsd";

	// Enable this for additional server debugging info.
	boolean debug = false;

	ProcessBuilder pb;
	Process p;
	ProcessStartInfo psInfo;

	class ProcessStartInfo {
		List<String> arguments = new ArrayList<String>();

		public ProcessStartInfo(String command) {
			this.arguments.add(command);
		}

		public void addArgument(String arg)
		{
			this.arguments.addAll(Arrays.asList(arg.split("\\s+")));
		}

		String[] getArgsAsArray() {
			return arguments.toArray(new String[arguments.size()]);
		}

		String getArgsAsString() {
			String stringVal = new String();
			for (String s : arguments)
				stringVal = stringVal.concat(s+" ");
			return stringVal.trim();
		}

		public String toString() {
			return getArgsAsString();
		}
	}

	public NATSServer()
	{
		this(-1);
	}


	public NATSServer(int port)
	{
		psInfo = this.createProcessStartInfo();

		if (port > 1023) {
			psInfo.addArgument("-p " + String.valueOf(port));
		}
		//        psInfo.addArgument("-m 8222");

		start();
	}

	private String buildConfigFileName(String configFile)
	{
		return configFile;
	}

	public NATSServer(String configFile)
	{
		psInfo = this.createProcessStartInfo();
		psInfo.addArgument("-config " + buildConfigFileName(configFile));
		start();
	}

	private ProcessStartInfo createProcessStartInfo()
	{
		psInfo = new ProcessStartInfo(GNATSD);

		if (debug)
		{
			psInfo.addArgument("-DV");
			//            psInfo.addArgument("-l gnatsd.log");
		}

		return psInfo;
	}

	public void start()
	{
		try {
			pb = new ProcessBuilder(psInfo.arguments);
			//pb.directory(new File("src/test"));
			if (debug)
				pb.inheritIO();
			else {
				pb.redirectError(new File("/dev/null"));
				pb.redirectOutput(new File("/dev/null"));
			}
			p = pb.start();
			if (debug)
				System.out.println("Started [" + psInfo + "]");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void shutdown()
	{
		if (p == null)
			return;

		p.destroy();
		if (debug)
			System.out.println("Stopped [" + psInfo + "]");

		p = null;
	}

	@Override
	public void close() {
		this.shutdown();
	}
}
