/*
 * Copyright 2023-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.core;

import org.springframework.kafka.KafkaException;

/**
 * Exception when no producer is available.
 *
 * @author Wang Zhiyang
 * @author Ilya Starchenko
 *
 * @since 3.2
 */
public class NoProducerAvailableException extends KafkaException {

	private static final long serialVersionUID = 1L;

	private final String txIdPrefix;

	/**
	 * Constructs a new no producer available exception with the specified detail message.
	 * @param message the message.
	 * @param txIdPrefix the transaction id prefix.
	 */
	public NoProducerAvailableException(String message, String txIdPrefix) {
		super(message);
		this.txIdPrefix = txIdPrefix;
	}

	/**
	 * Return the transaction id prefix that was used to create the producer and failed.
	 * @return the transaction id prefix.
	 */
	public String getTxIdPrefix() {
		return this.txIdPrefix;
	}

}
