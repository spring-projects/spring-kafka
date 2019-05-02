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

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

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
	 * Property name for the application context id.
	 */
	public static final String KSTREAM_DESERIALIZATION_CONTEXT_ID = "spring.deserialization.context.id";

	/**
	 * Property name for the bean name of the recoverer.
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
		if (configs.containsKey(KSTREAM_DESERIALIZATION_CONTEXT_ID)) {
			ApplicationContext context = StreamsBuilderFactoryBean
					.getApplicationContext((String) configs.get(KSTREAM_DESERIALIZATION_CONTEXT_ID));
			if (context != null) {
				String beanName = (String) configs.get(KSTREAM_DESERIALIZATION_RECOVERER);
				if (beanName != null) {
					try {
						this.recoverer = context.getBean(beanName, ConsumerRecordRecoverer.class);
					}
					catch (NoSuchBeanDefinitionException e) {
						LOGGER.error("Failed to retrieve recoverer from application context"
								+ "; failed deserializations cannot be recovered", e);
					}
				}
				else {
					LOGGER.error("No " + KSTREAM_DESERIALIZATION_RECOVERER + " property found"
							+ "; failed deserializations cannot be recovered");
				}
			}
			else {
				LOGGER.error("No application context with id " + configs.get(KSTREAM_DESERIALIZATION_CONTEXT_ID)
						+ "; failed deserializations cannot be recovered");
			}
		}
		else {
			LOGGER.error("No application context id property; failed deserializations cannot be recovered");
		}

	}

}
