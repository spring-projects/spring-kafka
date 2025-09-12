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

import org.jspecify.annotations.Nullable;

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
 * @since 4.0
 */
public class ShareKafkaListenerContainerFactory<K, V>
		implements KafkaListenerContainerFactory<ShareKafkaMessageListenerContainer<K, V>>, ApplicationEventPublisherAware, ApplicationContextAware {

	private final ShareConsumerFactory<? super K, ? super V> shareConsumerFactory;

	private @Nullable Boolean autoStartup;

	private @Nullable Integer phase;

	private @Nullable ApplicationEventPublisher applicationEventPublisher;

	private @Nullable ApplicationContext applicationContext;

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
	public void setAutoStartup(Boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * Set the phase in which containers created by this factory should start and stop.
	 * @param phase the phase
	 */
	public void setPhase(Integer phase) {
		this.phase = phase;
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
		Boolean effectiveAutoStartup = endpoint.getAutoStartup() != null ? endpoint.getAutoStartup() : this.autoStartup;

		// Validate share group configuration
		validateShareConfiguration(endpoint);

		Object o = this.shareConsumerFactory.getConfigurationProperties().get("share.acknowledgement.mode");
		String explicitAck = null;
		if (o != null) {
			explicitAck = (String) o;
		}
		JavaUtils.INSTANCE
				.acceptIfNotNull(effectiveAutoStartup, instance::setAutoStartup)
				.acceptIfNotNull(this.phase, instance::setPhase)
				.acceptIfNotNull(this.applicationContext, instance::setApplicationContext)
				.acceptIfNotNull(this.applicationEventPublisher, instance::setApplicationEventPublisher)
				.acceptIfNotNull(endpoint.getGroupId(), properties::setGroupId)
				.acceptIfNotNull(endpoint.getClientIdPrefix(), properties::setClientId)
				.acceptIfCondition(explicitAck != null && explicitAck.equals("explicit"),
						ContainerProperties.ShareAcknowledgmentMode.EXPLICIT,
						properties::setShareAcknowledgmentMode);
	}

	private void validateShareConfiguration(KafkaListenerEndpoint endpoint) {
		// Validate that batch listeners aren't used with share consumers
		if (endpoint.getBatchListener() != null && endpoint.getBatchListener()) {
			throw new IllegalArgumentException(
					"Batch listeners are not supported with share consumers. " +
							"Share groups operate at the record level.");
		}

		// Validate acknowledgment mode consistency
		Object ackMode = this.shareConsumerFactory.getConfigurationProperties()
				.get("share.acknowledgement.mode");
		if (ackMode != null && !Arrays.asList("implicit", "explicit").contains(ackMode)) {
			throw new IllegalArgumentException(
					"Invalid share.acknowledgement.mode: " + ackMode +
							". Must be 'implicit' or 'explicit'");
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
