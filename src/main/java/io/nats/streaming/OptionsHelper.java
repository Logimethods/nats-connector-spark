/*******************************************************************************
 * Copyright (c) 2017 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.streaming;

public class OptionsHelper {

	public static String extractNatsUrl(Options options) {
		return options.getNatsUrl();
	}

}
