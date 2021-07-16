/*
 * Copyright 2016-2021 the original author or authors.
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


import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;

/**
 * Simple handler that logs each record.
 *
 * @deprecated - use the {@link CommonLoggingErrorHandler} instead.
 *
 * @author Gary Russell
 * @since 1.1
 */
@Deprecated
public class BatchLoggingErrorHandler implements BatchErrorHandler {

	private static final LogAccessor LOGGER =
			new LogAccessor(LogFactory.getLog(BatchLoggingErrorHandler.class));

	@Override
	public void handle(Exception thrownException, @Nullable ConsumerRecords<?, ?> data) {
		StringBuilder message = new StringBuilder("Error while processing:\n");
		if (data == null) {
			message.append("null ");
		}
		else {
			for (ConsumerRecord<?, ?> record : data) {
				message.append(ListenerUtils.recordToString(record)).append('\n');
			}
		}
		LOGGER.error(thrownException, () -> message.substring(0, message.length() - 1));
	}

}
