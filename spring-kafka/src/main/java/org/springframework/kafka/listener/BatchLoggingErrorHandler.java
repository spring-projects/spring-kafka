/*
 * Copyright 2016-2017 the original author or authors.
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

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Simple handler that invokes a {@link LoggingErrorHandler} for each record.
 *
 * @author Gary Russell
 * @since 1.1
 *
 */
public class BatchLoggingErrorHandler implements BatchErrorHandler {

	private static final Log log = LogFactory.getLog(BatchLoggingErrorHandler.class);

	@Override
	public void handle(Exception thrownException, ConsumerRecords<?, ?> data) {
		StringBuilder message = new StringBuilder("Error while processing:\n");
		if (data == null) {
			message.append("null ");
		}
		else {
			Iterator<?> iterator = data.iterator();
			while (iterator.hasNext()) {
				message.append(iterator.next()).append('\n');
			}
		}
		log.error(message.substring(0, message.length() - 1), thrownException);
	}

}
