/*
 * Copyright 2015-2019 the original author or authors.
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

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.support.EnhancedLogFactory;
import org.springframework.kafka.support.EnhancedLogFactory.Log;
import org.springframework.util.ObjectUtils;

/**
 * The {@link ErrorHandler} implementation for logging purpose.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class LoggingErrorHandler implements ErrorHandler {

	private static final Log LOGGER = EnhancedLogFactory.getLog(LoggingErrorHandler.class);

	@Override
	public void handle(Exception thrownException, ConsumerRecord<?, ?> record) {
		LOGGER.error(() -> "Error while processing: " + ObjectUtils.nullSafeToString(record), thrownException);
	}

}
