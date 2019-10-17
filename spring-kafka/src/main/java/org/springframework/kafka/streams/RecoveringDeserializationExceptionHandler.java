/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.streams;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.util.ClassUtils;

/**
 * A {@link DeserializationExceptionHandler} that calls a {@link ConsumerRecordRecoverer}.
 * and continues.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class RecoveringDeserializationExceptionHandler implements DeserializationExceptionHandler {

	/**
	 * Property name for configuring the recoverer using properties.
	 */
	public static final String KSTREAM_DESERIALIZATION_RECOVERER = "spring.deserialization.recoverer";

	private static final Log LOGGER = LogFactory.getLog(RecoveringDeserializationExceptionHandler.class);

	private ConsumerRecordRecoverer recoverer;

	public RecoveringDeserializationExceptionHandler() {
		super();
	}

	public RecoveringDeserializationExceptionHandler(ConsumerRecordRecoverer recoverer) {
		this.recoverer = recoverer;
	}

	@Override
	public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record,
			Exception exception) {

		if (this.recoverer == null) {
			return DeserializationHandlerResponse.FAIL;
		}
		try {
			this.recoverer.accept(record, exception);
			return DeserializationHandlerResponse.CONTINUE;
		}
		catch (RuntimeException e) {
			LOGGER.error("Recoverer threw an exception; recovery failed", e);
			return DeserializationHandlerResponse.FAIL;
		}
	}

	@Override
	public void configure(Map<String, ?> configs) {
		if (configs.containsKey(KSTREAM_DESERIALIZATION_RECOVERER)) {
			Object configValue = configs.get(KSTREAM_DESERIALIZATION_RECOVERER);
			if (configValue instanceof ConsumerRecordRecoverer) {
				this.recoverer = (ConsumerRecordRecoverer) configValue;
			}
			else if (configValue instanceof String) {
				fromString(configValue);
			}
			else if (configValue instanceof Class) {
				fromClass(configValue);
			}
			else {
				LOGGER.error("Unkown property type for " + KSTREAM_DESERIALIZATION_RECOVERER
						+ "; failed deserializations cannot be recovered");
			}
		}

	}

	private void fromString(Object configValue) throws LinkageError {
		try {
			@SuppressWarnings("unchecked")
			Class<? extends ConsumerRecordRecoverer> clazz =
					(Class<? extends ConsumerRecordRecoverer>) ClassUtils
						.forName((String) configValue,
									RecoveringDeserializationExceptionHandler.class.getClassLoader());
			Constructor<? extends ConsumerRecordRecoverer> constructor = clazz.getConstructor();
			this.recoverer = constructor.newInstance();
		}
		catch (Exception e) {
			LOGGER.error("Failed to instantiate recoverer, failed deserializations cannot be recovered", e);
		}
	}

	private void fromClass(Object configValue) {
		try {
			@SuppressWarnings("unchecked")
			Class<? extends ConsumerRecordRecoverer> clazz =
				(Class<? extends ConsumerRecordRecoverer>) configValue;
			Constructor<? extends ConsumerRecordRecoverer> constructor = clazz.getConstructor();
			this.recoverer = constructor.newInstance();
		}
		catch (Exception e) {
			LOGGER.error("Failed to instantiate recoverer, failed deserializations cannot be recovered", e);
		}
	}

}
