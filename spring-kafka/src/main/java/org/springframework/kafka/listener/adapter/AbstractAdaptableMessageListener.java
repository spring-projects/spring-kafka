/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.MessageListener;

/**
 * An abstract {@link MessageListener} adapter providing the necessary infrastructure
 * to extract the payload of a {@link org.springframework.messaging.Message}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @see MessageListener
 * @see AcknowledgingMessageListener
 */
public abstract class AbstractAdaptableMessageListener<K, V> implements MessageListener<K, V>,
			AcknowledgingMessageListener<K, V> {

	protected final Log logger = LogFactory.getLog(getClass()); //NOSONAR

	/**
	 * Kafka {@link MessageListener} entry point.
	 * <p> Delegate the message to the target listener method,
	 * with appropriate conversion of the message argument.
	 * @param record the incoming Kafka {@link ConsumerRecord}.
	 * @see #handleListenerException
	 * @see AcknowledgingMessageListener#onMessage(ConsumerRecord, org.springframework.kafka.support.Acknowledgment)
	 */
	@Override
	public void onMessage(ConsumerRecord<K, V> record) {
		onMessage(record, null);
	}

	/**
	 * Handle the given exception that arose during listener execution.
	 * Can be used by inheritors from overridden {@link #onMessage(ConsumerRecord)}
	 * or {@link #onMessage(ConsumerRecord, org.springframework.kafka.support.Acknowledgment)}
	 * The default implementation logs the exception at error level.
	 * @param ex the exception to handle
	 */
	protected void handleListenerException(Throwable ex) {
		this.logger.error("Listener execution failed", ex);
	}

	/**
	 * Extract the message body from the given Kafka message.
	 * @param record the Kafka <code>Message</code>
	 * @return the content of the message, to be passed into the listener method as argument
	 */
	protected Object extractMessage(ConsumerRecord<K, V> record) {
		return record.value();
	}

}
