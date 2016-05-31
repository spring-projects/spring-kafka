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

import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.ContainerProperties;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.adapter.DeDuplicationStrategy;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;

/**
 * Base {@link KafkaListenerContainerFactory} for Spring's base container implementation.
 *
 * @param <C> the {@link AbstractMessageListenerContainer} implementation type.
 * @param <K> the key type.
 * @param <V> the value type.
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

	private AsyncListenableTaskExecutor consumerTaskExecutor;

	private AsyncListenableTaskExecutor listenerTaskExecutor;

	private Integer ackCount;

	private AckMode ackMode;

	private Long pollTimeout;

	private MessageConverter messageConverter;

	private DeDuplicationStrategy<K, V> deDuplicationStrategy;

	private Boolean pauseEnabled;

	private Long pauseAfter;

	private RetryTemplate retryTemplate;

	private RecoveryCallback<Void> recoveryCallback;

	/**
	 * Specify a {@link ConsumerFactory} to use.
	 * @param consumerFactory The consumer factory.
	 */
	public void setConsumerFactory(ConsumerFactory<K, V> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	public ConsumerFactory<K, V> getConsumerFactory() {
		return this.consumerFactory;
	}

	/**
	 * Specify an {@link ErrorHandler} to use.
	 * @param errorHandler The error handler.
	 * @see ContainerProperties#setErrorHandler(ErrorHandler)
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Specify an {@link Executor} to use.
	 * @param consumerTaskExecutor the {@link Executor} to use to poll
	 * Kafka.
	 * @see ContainerProperties#setConsumerTaskExecutor
	 */
	public void setConsumerTaskExecutor(AsyncListenableTaskExecutor consumerTaskExecutor) {
		this.consumerTaskExecutor = consumerTaskExecutor;
	}

	/**
	 * Specify an {@link Executor} to use.
	 * @param listenerTaskExecutor the {@link Executor} to use to invoke
	 * the listener.
	 * @see ContainerProperties#setListenerTaskExecutor
	 */
	public void setListenerTaskExecutor(AsyncListenableTaskExecutor listenerTaskExecutor) {
		this.listenerTaskExecutor = listenerTaskExecutor;
	}

	/**
	 * Specify an {@code autoStartup boolean} flag.
	 * @param autoStartup true for auto startup.
	 * @see ContainerProperties#setAutoStartup(boolean)
	 */
	public void setAutoStartup(Boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * Specify a {@code phase} to use.
	 * @param phase The phase.
	 * @see ContainerProperties#setPhase(int)
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * Specify an {@code ackCount} to use.
	 * @param ackCount the ack count.
	 * @see ContainerProperties#setAckCount(int)
	 */
	public void setAckCount(Integer ackCount) {
		this.ackCount = ackCount;
	}

	/**
	 * Specify an {@link AckMode} to use.
	 * @param ackMode the ack mode.
	 * @see ContainerProperties#ackMode
	 */
	public void setAckMode(AckMode ackMode) {
		this.ackMode = ackMode;
	}

	/**
	 * Specify a {@code pollTimeout} to use.
	 * @param pollTimeout the poll timeout
	 * @see ContainerProperties#setPollTimeout(long)
	 */
	public void setPollTimeout(Long pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	/**
	 * Set the message converter to use if dynamic argument type matching is needed.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Set the de-duplication strategy.
	 * @param deDuplicationStrategy the strategy.
	 */
	public void setDeDuplicationStrategy(DeDuplicationStrategy<K, V> deDuplicationStrategy) {
		this.deDuplicationStrategy = deDuplicationStrategy;
	}

	/**
	 * Set to true to enable pausing the consumer.
	 * @param pauseEnabled the pauseWhenSlow to set.
	 * @see ContainerProperties#setPauseEnabled(boolean)
	 */
	public void setPauseEnabled(boolean pauseEnabled) {
		this.pauseEnabled = pauseEnabled;
	}

	/**
	 * Set the time after which the consumer should be paused.
	 * @param pauseAfter the pauseAfter to set.
	 * @see ContainerProperties#setPauseAfter(long)
	 */
	public void setPauseAfter(long pauseAfter) {
		this.pauseAfter = pauseAfter;
	}

	public void setRetryTemplate(RetryTemplate retryTemplate) {
		this.retryTemplate = retryTemplate;
	}

	public void setRecoveryCallback(RecoveryCallback<Void> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	@SuppressWarnings("unchecked")
	@Override
	public C createListenerContainer(KafkaListenerEndpoint endpoint) {
		C instance = createContainerInstance(endpoint);

		if (this.autoStartup != null) {
			instance.setAutoStartup(this.autoStartup);
		}
		if (this.phase != null) {
			instance.setPhase(this.phase);
		}
		if (endpoint.getId() != null) {
			instance.setBeanName(endpoint.getId());
		}
		ContainerProperties properties = new ContainerProperties();
		if (this.consumerTaskExecutor != null) {
			properties.setConsumerTaskExecutor(this.consumerTaskExecutor);
		}
		if (this.listenerTaskExecutor != null) {
			properties.setListenerTaskExecutor(this.listenerTaskExecutor);
		}
		if (this.errorHandler != null) {
			properties.setErrorHandler(this.errorHandler);
		}
		if (this.ackCount != null) {
			properties.setAckCount(this.ackCount);
		}
		if (this.ackMode != null) {
			properties.setAckMode(this.ackMode);
		}
		if (this.pollTimeout != null) {
			properties.setPollTimeout(this.pollTimeout);
		}
		if (this.pauseEnabled != null) {
			properties.setPauseEnabled(this.pauseEnabled);
		}
		if (this.pauseAfter != null) {
			properties.setPauseAfter(this.pauseAfter);
		}
		if (this.retryTemplate != null) {
			properties.setRetryTemplate(this.retryTemplate);
		}
		if (this.recoveryCallback != null) {
			properties.setRecoveryCallback(this.recoveryCallback);
		}

		if (this.deDuplicationStrategy != null && endpoint instanceof AbstractKafkaListenerEndpoint) {
			((AbstractKafkaListenerEndpoint<K, V>) endpoint).setDeDuplicationStrategy(this.deDuplicationStrategy);
		}

		instance.setContainerProperties(properties);
		endpoint.setupListenerContainer(instance, this.messageConverter);
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
