/*
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.config;


import java.util.concurrent.Executor;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.KafkaListenerContainerFactory;
import org.springframework.kafka.listener.KafkaListenerEndpoint;

/**
 * Base {@link KafkaListenerContainerFactory} for Spring's base container implementation.
 *
 * @author Stephane Nicoll
 *
 * @see AbstractMessageListenerContainer
 */
public abstract class AbstractKafkaListenerContainerFactory<C extends AbstractMessageListenerContainer<K, V>, K, V>
		implements KafkaListenerContainerFactory<C> {

	private ConsumerFactory<K, V> consumerFactory;

	private ErrorHandler errorHandler;

	private Boolean autoStartup;

	private Integer phase;

	private Executor taskExecutor;

	private Integer ackCount;

	private AckMode ackMode;

	private Long pollTimeout;

	/**
	 * @param consumerFactory The consumer factory.
	 */
	public void setConsumerFactory(ConsumerFactory<K, V> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	public ConsumerFactory<K, V> getConsumerFactory() {
		return this.consumerFactory;
	}

	/**
	 * @param errorHandler The error handler.
	 * @see AbstractMessageListenerContainer#setErrorHandler(ErrorHandler)
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * @param taskExecutor the {@link Executor} to use.
	 * @see AbstractKafkaListenerContainerFactory#setTaskExecutor
	 */
	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * @param autoStartup true for auto startup.
	 * @see AbstractMessageListenerContainer#setAutoStartup(boolean)
	 */
	public void setAutoStartup(Boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * @param phase The phase.
	 * @see AbstractMessageListenerContainer#setPhase(int)
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * @param ackCount the ack count.
	 * @see AbstractMessageListenerContainer#setAckCount(int)
	 */
	public void setAckCount(Integer ackCount) {
		this.ackCount = ackCount;
	}

	/**
	 * @param ackMode the ack mode.
	 * @see AbstractMessageListenerContainer#setAckMode(AckMode)
	 */
	public void setAckMode(AckMode ackMode) {
		this.ackMode = ackMode;
	}

	/**
	 * @param pollTimeout the poll timeout
	 * @see AbstractMessageListenerContainer#setPollTimeout(long)
	 */
	public void setPollTimeout(Long pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	@Override
	public C createListenerContainer(KafkaListenerEndpoint endpoint) {
		C instance = createContainerInstance(endpoint);

		if (this.taskExecutor != null) {
			instance.setTaskExecutor(this.taskExecutor);
		}
		if (this.errorHandler != null) {
			instance.setErrorHandler(this.errorHandler);
		}
		if (this.autoStartup != null) {
			instance.setAutoStartup(this.autoStartup);
		}
		if (this.phase != null) {
			instance.setPhase(this.phase);
		}
		if (this.ackCount != null) {
			instance.setAckCount(this.ackCount);
		}
		if (this.ackMode != null) {
			instance.setAckMode(this.ackMode);
		}
		if (endpoint.getId() != null) {
			instance.setBeanName(endpoint.getId());
		}
		if (this.pollTimeout != null) {
			instance.setPollTimeout(this.pollTimeout);
		}

		endpoint.setupListenerContainer(instance);
		initializeContainer(instance);

		return instance;
	}

	/**
	 * Create an empty container instance.
	 * @param endpoint the endpoint.
	 * @return the new container instance.
	 */
	protected abstract C createContainerInstance(KafkaListenerEndpoint endpoint);

	/**
	 * Further initialize the specified container.
	 * <p>Subclasses can inherit from this method to apply extra
	 * configuration if necessary.
	 * @param instance the container instance to configure.
	 */
	protected void initializeContainer(C instance) {
	}

}
