/*
 * Copyright 2025-present the original author or authors.
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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode;

import org.springframework.beans.BeanUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ShareKafkaMessageListenerContainer;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.Assert;

/**
 * A {@link KafkaListenerContainerFactory} implementation to create {@link ShareKafkaMessageListenerContainer}
 * instances for Kafka's share consumer model.
 * <p>
 * This factory provides common configuration and lifecycle management for share consumer containers.
 * It handles the creation of containers based on endpoints, topics, or patterns, and applies common
 * configuration properties to the created containers.
 * <p>
 * The share consumer model enables cooperative rebalancing, allowing consumers to maintain ownership of
 * some partitions while relinquishing others during rebalances, which can reduce disruption compared to
 * the classic consumer model.
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @author Soby Chacko
 *
 * @since 4.0
 */
public class ShareKafkaListenerContainerFactory<K, V>
		implements KafkaListenerContainerFactory<ShareKafkaMessageListenerContainer<K, V>>,
		ApplicationEventPublisherAware, ApplicationContextAware {

	private final ShareConsumerFactory<? super K, ? super V> shareConsumerFactory;

	private final ContainerProperties containerProperties = new ContainerProperties((Pattern) null);

	private boolean autoStartup = true;

	private int phase = 0;

	private int concurrency = 1;

	@SuppressWarnings("NullAway.Init")
	private ApplicationEventPublisher applicationEventPublisher;

	@SuppressWarnings("NullAway.Init")
	private ApplicationContext applicationContext;

	/**
	 * Construct an instance with the provided consumer factory.
	 * @param shareConsumerFactory the share consumer factory
	 */
	public ShareKafkaListenerContainerFactory(ShareConsumerFactory<K, V> shareConsumerFactory) {
		this.shareConsumerFactory = shareConsumerFactory;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	/**
	 * Set whether containers created by this factory should auto-start.
	 * @param autoStartup true to auto-start
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * Set the phase in which containers created by this factory should start and stop.
	 * @param phase the phase
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * Set the concurrency for containers created by this factory.
	 * <p>
	 * This specifies the number of consumer threads to create within each container.
	 * Each thread creates its own {@link org.apache.kafka.clients.consumer.ShareConsumer}
	 * instance and participates in the same share group. The Kafka broker distributes
	 * records across all consumer instances, providing record-level load balancing.
	 * <p>
	 * This can be overridden per listener endpoint using the {@code concurrency}
	 * attribute on {@code @KafkaListener}.
	 * @param concurrency the number of consumer threads (must be greater than 0)
	 */
	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	/**
	 * Obtain the factory-level container properties - set properties as needed
	 * and they will be copied to each listener container instance created by this factory.
	 * @return the properties.
	 */
	public ContainerProperties getContainerProperties() {
		return this.containerProperties;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public ShareKafkaMessageListenerContainer<K, V> createListenerContainer(KafkaListenerEndpoint endpoint) {
		ShareKafkaMessageListenerContainer<K, V> instance = createContainerInstance(endpoint);
		JavaUtils.INSTANCE
				.acceptIfNotNull(endpoint.getId(), instance::setBeanName);
		if (endpoint instanceof AbstractKafkaListenerEndpoint abstractKafkaListenerEndpoint) {
			configureEndpoint(abstractKafkaListenerEndpoint);
		}
		// TODO: No message converter for queue at the moment
		endpoint.setupListenerContainer(instance, null);
		initializeContainer(instance, endpoint);
		return instance;
	}

	private void configureEndpoint(AbstractKafkaListenerEndpoint<K, V> endpoint) {
		// Minimal configuration; can add more properties later
	}

	/**
	 * Initialize the provided container with common configuration properties.
	 * @param instance the container instance
	 * @param endpoint the endpoint
	 */
	protected void initializeContainer(ShareKafkaMessageListenerContainer<K, V> instance, KafkaListenerEndpoint endpoint) {
		ContainerProperties properties = instance.getContainerProperties();
		boolean effectiveAutoStartup = endpoint.getAutoStartup() != null ? endpoint.getAutoStartup() : this.autoStartup;

		// Validate share group configuration
		validateShareConfiguration(endpoint);

		// Copy factory-level properties to container
		BeanUtils.copyProperties(this.containerProperties, properties, "topics", "topicPartitions", "topicPattern",
				"messageListener", "ackCount", "ackTime", "subBatchPerPartition", "kafkaConsumerProperties");

		// Determine acknowledgment mode following Spring Kafka's configuration precedence patterns
		// Check factory-level properties first, then consumer factory config
		boolean explicitAck = determineExplicitAcknowledgment(properties);
		properties.setExplicitShareAcknowledgment(explicitAck);

		// Set concurrency - endpoint setting takes precedence over factory setting
		Integer conc = endpoint.getConcurrency();
		if (conc != null) {
			instance.setConcurrency(conc);
		}
		else {
			instance.setConcurrency(this.concurrency);
		}

		instance.setAutoStartup(effectiveAutoStartup);
		instance.setPhase(this.phase);
		instance.setApplicationContext(this.applicationContext);
		instance.setApplicationEventPublisher(this.applicationEventPublisher);

		JavaUtils.INSTANCE
				.acceptIfNotNull(endpoint.getGroupId(), properties::setGroupId)
				.acceptIfNotNull(endpoint.getClientIdPrefix(), properties::setClientId);
	}

	/**
	 * Determine whether explicit acknowledgment is required following Spring Kafka's configuration precedence patterns.
	 * <p>
	 * Configuration precedence (highest to lowest):
	 * <ol>
	 * <li>Container Properties: {@code containerProperties.isExplicitShareAcknowledgment()} (if explicitly set via factory-level properties)</li>
	 * <li>Consumer Config: {@code ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG}</li>
	 * <li>Default: {@code false} (implicit acknowledgment)</li>
	 * </ol>
	 * @param containerProperties the container properties to check
	 * @return true if explicit acknowledgment is required, false for implicit
	 * @throws IllegalArgumentException if an invalid acknowledgment mode is configured
	 */
	private boolean determineExplicitAcknowledgment(ContainerProperties containerProperties) {
		// Check factory-level properties first
		// If explicitly set to true (non-default), use it with highest precedence
		if (this.containerProperties.isExplicitShareAcknowledgment()) {
			return true;
		}

		// Check Kafka client configuration as fallback
		Object clientAckMode = this.shareConsumerFactory.getConfigurationProperties()
				.get(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG);

		if (clientAckMode != null) {
			ShareAcknowledgementMode mode = ShareAcknowledgementMode.fromString(clientAckMode.toString());
			return mode == ShareAcknowledgementMode.EXPLICIT;
		}

		// Default to implicit acknowledgment (false)
		return false;
	}

	private static void validateShareConfiguration(KafkaListenerEndpoint endpoint) {
		// Validate that batch listeners aren't used with share consumers
		if (Boolean.TRUE.equals(endpoint.getBatchListener())) {
			throw new IllegalArgumentException(
					"Batch listeners are not supported with share consumers. " +
							"Share groups operate at the record level.");
		}

	}

	@Override
	public ShareKafkaMessageListenerContainer<K, V> createContainer(TopicPartitionOffset... topicPartitions) {
		throw new UnsupportedOperationException("ShareConsumer does not support explicit partition assignment");
	}

	@Override
	public ShareKafkaMessageListenerContainer<K, V> createContainer(String... topics) {
		return createContainerInstance(new KafkaListenerEndpointAdapter() {

			@Override
			public Collection<String> getTopics() {
				return Arrays.asList(topics);
			}
		});
	}

	@Override
	public ShareKafkaMessageListenerContainer<K, V> createContainer(Pattern topicPattern) {
		throw new UnsupportedOperationException("ShareConsumer does not support topic patterns");
	}

	/**
	 * Create a container instance for the provided endpoint.
	 * @param endpoint the endpoint
	 * @return the container instance
	 */
	protected ShareKafkaMessageListenerContainer<K, V> createContainerInstance(KafkaListenerEndpoint endpoint) {
		Collection<String> topics = endpoint.getTopics();
		Assert.state(topics != null, "'topics' must not be null");
		return new ShareKafkaMessageListenerContainer<>(this.shareConsumerFactory,
				new ContainerProperties(topics.toArray(new String[0])));
	}

}
