/*
 * Copyright 2014-2019 the original author or authors.
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


import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.AfterRollbackProcessor;
import org.springframework.kafka.listener.BatchErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.GenericErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.listener.adapter.ReplyHeadersConfigurer;
import org.springframework.kafka.requestreply.ReplyingKafkaOperations;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * Base {@link KafkaListenerContainerFactory} for Spring's base container implementation.
 *
 * @param <C> the {@link AbstractMessageListenerContainer} implementation type.
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @see AbstractMessageListenerContainer
 */
public abstract class AbstractKafkaListenerContainerFactory<C extends AbstractMessageListenerContainer<K, V>, K, V>
		implements KafkaListenerContainerFactory<C>, ApplicationEventPublisherAware, InitializingBean,
			ApplicationContextAware {

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR protected

	private final ContainerProperties containerProperties = new ContainerProperties((Pattern) null);

	private GenericErrorHandler<?> errorHandler;

	private ConsumerFactory<? super K, ? super V> consumerFactory;

	private Boolean autoStartup;

	private Integer phase;

	private MessageConverter messageConverter;

	private RecordFilterStrategy<? super K, ? super V> recordFilterStrategy;

	private Boolean ackDiscarded;

	private RetryTemplate retryTemplate;

	private RecoveryCallback<? extends Object> recoveryCallback;

	private Boolean statefulRetry;

	private Boolean batchListener;

	private ApplicationEventPublisher applicationEventPublisher;

	private KafkaTemplate<?, ?> replyTemplate;

	private AfterRollbackProcessor<? super K, ? super V> afterRollbackProcessor;

	private ReplyHeadersConfigurer replyHeadersConfigurer;

	private Boolean missingTopicsFatal;

	private RecordInterceptor<K, V> recordInterceptor;

	private ApplicationContext applicationContext;


	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	/**
	 * Specify a {@link ConsumerFactory} to use.
	 * @param consumerFactory The consumer factory.
	 */
	public void setConsumerFactory(ConsumerFactory<? super K, ? super V> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	public ConsumerFactory<? super K, ? super V> getConsumerFactory() {
		return this.consumerFactory;
	}

	/**
	 * Specify an {@code autoStartup boolean} flag.
	 * @param autoStartup true for auto startup.
	 * @see AbstractMessageListenerContainer#setAutoStartup(boolean)
	 */
	public void setAutoStartup(Boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * Specify a {@code phase} to use.
	 * @param phase The phase.
	 * @see AbstractMessageListenerContainer#setPhase(int)
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * Set the message converter to use if dynamic argument type matching is needed.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Set the record filter strategy.
	 * @param recordFilterStrategy the strategy.
	 */
	public void setRecordFilterStrategy(RecordFilterStrategy<? super K, ? super V> recordFilterStrategy) {
		this.recordFilterStrategy = recordFilterStrategy;
	}

	/**
	 * Set to true to ack discards when a filter strategy is in use.
	 * @param ackDiscarded the ackDiscarded.
	 */
	public void setAckDiscarded(Boolean ackDiscarded) {
		this.ackDiscarded = ackDiscarded;
	}

	/**
	 * Set a retryTemplate.
	 * @param retryTemplate the template.
	 */
	public void setRetryTemplate(RetryTemplate retryTemplate) {
		this.retryTemplate = retryTemplate;
	}

	/**
	 * Set a callback to be used with the {@link #setRetryTemplate(RetryTemplate)
	 * retryTemplate}.
	 * @param recoveryCallback the callback.
	 */
	public void setRecoveryCallback(RecoveryCallback<? extends Object> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	/**
	 * When using a {@link RetryTemplate} Set to true to enable stateful retry. Use in
	 * conjunction with a
	 * {@link org.springframework.kafka.listener.SeekToCurrentErrorHandler} when retry can
	 * take excessive time; each failure goes back to the broker, to keep the Consumer
	 * alive.
	 * @param statefulRetry true to enable stateful retry.
	 * @since 2.1.3
	 */
	public void setStatefulRetry(boolean statefulRetry) {
		this.statefulRetry = statefulRetry;
	}


	/**
	 * Return true if this endpoint creates a batch listener.
	 * @return true for a batch listener.
	 * @since 1.1
	 */
	public Boolean isBatchListener() {
		return this.batchListener;
	}

	/**
	 * Set to true if this endpoint should create a batch listener.
	 * @param batchListener true for a batch listener.
	 * @since 1.1
	 */
	public void setBatchListener(Boolean batchListener) {
		this.batchListener = batchListener;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	/**
	 * Set the {@link KafkaTemplate} to use to send replies.
	 * @param replyTemplate the template.
	 * @since 2.0
	 */
	public void setReplyTemplate(KafkaTemplate<?, ?> replyTemplate) {
		if (replyTemplate instanceof ReplyingKafkaOperations) {
			this.logger.warn(
					"The 'replyTemplate' should not be an implementation of 'ReplyingKafkaOperations'; "
							+ "such implementations are for client-side request/reply operations; here we "
							+ "are simply sending a reply to an incoming request so the reply container will "
							+ "never be used and will consume unnecessary resources.");
		}
		this.replyTemplate = replyTemplate;
	}

	/**
	 * Set the error handler to call when the listener throws an exception.
	 * @param errorHandler the error handler.
	 * @since 2.2
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Set the batch error handler to call when the listener throws an exception.
	 * @param errorHandler the error handler.
	 * @since 2.2
	 */
	public void setBatchErrorHandler(BatchErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Set a processor to invoke after a transaction rollback; typically will
	 * seek the unprocessed topic/partition to reprocess the records.
	 * The default does so, including the failed record.
	 * @param afterRollbackProcessor the processor.
	 * @since 1.3.5
	 */
	public void setAfterRollbackProcessor(AfterRollbackProcessor<? super K, ? super V> afterRollbackProcessor) {
		this.afterRollbackProcessor = afterRollbackProcessor;
	}

	/**
	 * Set a configurer which will be invoked when creating a reply message.
	 * @param replyHeadersConfigurer the configurer.
	 * @since 2.2
	 */
	public void setReplyHeadersConfigurer(ReplyHeadersConfigurer replyHeadersConfigurer) {
		this.replyHeadersConfigurer = replyHeadersConfigurer;
	}

	/**
	 * Set to false to allow the container to start even if any of the configured topics
	 * are not present on the broker. Does not apply when topic patterns are configured.
	 * Default true;
	 * @param missingTopicsFatal the missingTopicsFatal.
	 * @since 2.3
	 */
	public void setMissingTopicsFatal(boolean missingTopicsFatal) {
		this.missingTopicsFatal = missingTopicsFatal;
	}

	/**
	 * Obtain the properties template for this factory - set properties as needed
	 * and they will be copied to a final properties instance for the endpoint.
	 * @return the properties.
	 */
	public ContainerProperties getContainerProperties() {
		return this.containerProperties;
	}

	/**
	 * Set an interceptor to be called before calling the listener.
	 * Does not apply to batch listeners.
	 * @param recordInterceptor the interceptor.
	 * @since 2.2.7
	 */
	public void setRecordInterceptor(RecordInterceptor<K, V> recordInterceptor) {
		this.recordInterceptor = recordInterceptor;
	}

	@Override
	public void afterPropertiesSet() {
		if (this.errorHandler != null) {
			if (Boolean.TRUE.equals(this.batchListener)) {
				Assert.state(this.errorHandler instanceof BatchErrorHandler,
						() -> "The error handler must be a BatchErrorHandler, not " +
								this.errorHandler.getClass().getName());
			}
			else {
				Assert.state(this.errorHandler instanceof ErrorHandler,
						() -> "The error handler must be an ErrorHandler, not " +
								this.errorHandler.getClass().getName());
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public C createListenerContainer(KafkaListenerEndpoint endpoint) {
		C instance = createContainerInstance(endpoint);
		JavaUtils.INSTANCE
				.acceptIfNotNull(endpoint.getId(), instance::setBeanName);
		if (endpoint instanceof AbstractKafkaListenerEndpoint) {
			configureEndpoint((AbstractKafkaListenerEndpoint<K, V>) endpoint);
		}

		endpoint.setupListenerContainer(instance, this.messageConverter);
		initializeContainer(instance, endpoint);

		return instance;
	}

	private void configureEndpoint(AbstractKafkaListenerEndpoint<K, V> aklEndpoint) {
		JavaUtils.INSTANCE
				.acceptIfNotNull(this.recordFilterStrategy, aklEndpoint::setRecordFilterStrategy)
				.acceptIfNotNull(this.ackDiscarded, aklEndpoint::setAckDiscarded)
				.acceptIfNotNull(this.retryTemplate, aklEndpoint::setRetryTemplate)
				.acceptIfNotNull(this.recoveryCallback, aklEndpoint::setRecoveryCallback)
				.acceptIfNotNull(this.statefulRetry, aklEndpoint::setStatefulRetry)
				.acceptIfNotNull(this.batchListener, aklEndpoint::setBatchListener)
				.acceptIfNotNull(this.replyTemplate, aklEndpoint::setReplyTemplate)
				.acceptIfNotNull(this.replyHeadersConfigurer, aklEndpoint::setReplyHeadersConfigurer);
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
	 * @param endpoint the endpoint.
	 */
	protected void initializeContainer(C instance, KafkaListenerEndpoint endpoint) {
		ContainerProperties properties = instance.getContainerProperties();
		BeanUtils.copyProperties(this.containerProperties, properties, "topics", "topicPartitions", "topicPattern",
				"messageListener", "ackCount", "ackTime");
		JavaUtils.INSTANCE
				.acceptIfNotNull(this.afterRollbackProcessor, instance::setAfterRollbackProcessor)
				.acceptIfCondition(this.containerProperties.getAckCount() > 0, this.containerProperties.getAckCount(),
						properties::setAckCount)
				.acceptIfCondition(this.containerProperties.getAckTime() > 0, this.containerProperties.getAckTime(),
						properties::setAckTime)
				.acceptIfNotNull(this.errorHandler, instance::setGenericErrorHandler)
				.acceptIfNotNull(this.missingTopicsFatal, instance.getContainerProperties()::setMissingTopicsFatal);
		if (endpoint.getAutoStartup() != null) {
			instance.setAutoStartup(endpoint.getAutoStartup());
		}
		else if (this.autoStartup != null) {
			instance.setAutoStartup(this.autoStartup);
		}
		instance.setRecordInterceptor(this.recordInterceptor);
		JavaUtils.INSTANCE
				.acceptIfNotNull(this.phase, instance::setPhase)
				.acceptIfNotNull(this.applicationContext, instance::setApplicationContext)
				.acceptIfNotNull(this.applicationEventPublisher, instance::setApplicationEventPublisher)
				.acceptIfNotNull(endpoint.getGroupId(), instance.getContainerProperties()::setGroupId)
				.acceptIfNotNull(endpoint.getClientIdPrefix(), instance.getContainerProperties()::setClientId)
				.acceptIfNotNull(endpoint.getConsumerProperties(),
						instance.getContainerProperties()::setKafkaConsumerProperties);
	}

	/**
	 * {@inheritDoc}
	 * @deprecated in favor of {@link #createContainer(TopicPartitionOffset[])}
	 */
	@Deprecated
	@Override
	public C createContainer(Collection<org.springframework.kafka.support.TopicPartitionInitialOffset> topicPartitions) {
		return createContainer(topicPartitions.stream()
				.map(org.springframework.kafka.support.TopicPartitionInitialOffset::toTPO)
				.toArray(TopicPartitionOffset[]::new));
	}

	@Override
	public C createContainer(TopicPartitionOffset... topicsAndPartitions) {
		KafkaListenerEndpoint endpoint = new KafkaListenerEndpointAdapter() {

			@Override
			public TopicPartitionOffset[] getTopicPartitionsToAssign() {
				return Arrays.copyOf(topicsAndPartitions, topicsAndPartitions.length);
			}

		};
		C container = createContainerInstance(endpoint);
		initializeContainer(container, endpoint);
		return container;
	}

	@Override
	public C createContainer(String... topics) {
		KafkaListenerEndpoint endpoint = new KafkaListenerEndpointAdapter() {

			@Override
			public Collection<String> getTopics() {
				return Arrays.asList(topics);
			}

		};
		C container = createContainerInstance(endpoint);
		initializeContainer(container, endpoint);
		return container;
	}

	@Override
	public C createContainer(Pattern topicPattern) {
		KafkaListenerEndpoint endpoint = new KafkaListenerEndpointAdapter() {

			@Override
			public Pattern getTopicPattern() {
				return topicPattern;
			}

		};
		C container = createContainerInstance(endpoint);
		initializeContainer(container, endpoint);
		return container;
	}

}
