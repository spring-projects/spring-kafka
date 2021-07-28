/*
 * Copyright 2021 the original author or authors.
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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.core.log.LogAccessor;

/**
 * The {@link CommonErrorHandler} implementation for logging exceptions.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public class CommonLoggingErrorHandler implements CommonErrorHandler {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(CommonLoggingErrorHandler.class));

	private boolean ackAfterHandle = true;

	@Override
	public boolean isAckAfterHandle() {
		return this.ackAfterHandle;
	}

	@Override
	public void setAckAfterHandle(boolean ackAfterHandle) {
		this.ackAfterHandle = ackAfterHandle;
	}

	@Override
	public void handleRecord(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		LOGGER.error(thrownException, () -> "Error occured while processing: " + ListenerUtils.recordToString(record));
	}

	@Override
	public void handleBatch(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener) {

		StringBuilder message = new StringBuilder("Error occurred while processing:\n");
		for (ConsumerRecord<?, ?> record : data) {
			message.append(ListenerUtils.recordToString(record)).append('\n');
		}
		LOGGER.error(thrownException, () -> message.substring(0, message.length() - 1));
	}

	@Override
	public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer,
			MessageListenerContainer container, boolean batchListener) {

		LOGGER.error(thrownException, () -> "Error occurred while not processing records");
	}

}
