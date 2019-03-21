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

package org.springframework.kafka.config;


import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.converter.MessageConverter;

/**
 * A {@link KafkaListenerEndpoint} simply providing the {@link MessageListener} to
 * invoke to process an incoming message for this endpoint.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public class SimpleKafkaListenerEndpoint<K, V> extends AbstractKafkaListenerEndpoint<K, V> {

	private MessageListener<K, V> messageListener;


	/**
	 * Set the {@link MessageListener} to invoke when a message matching
	 * the endpoint is received.
	 * @param messageListener the {@link MessageListener} instance.
	 */
	public void setMessageListener(MessageListener<K, V> messageListener) {
		this.messageListener = messageListener;
	}

	/**
	 * Return the {@link MessageListener} to invoke when a message matching
	 * the endpoint is received.
	 * @return the {@link MessageListener} to invoke when a message matching
	 * the endpoint is received.
	 */
	public MessageListener<K, V> getMessageListener() {
		return this.messageListener;
	}


	@Override
	protected MessageListener<K, V> createMessageListener(MessageListenerContainer container,
			MessageConverter messageConverter) {
		return getMessageListener();
	}

	@Override
	protected StringBuilder getEndpointDescription() {
		return super.getEndpointDescription()
				.append(" | messageListener='").append(this.messageListener).append("'");
	}

}
