/*
 * Copyright 2017-2021 the original author or authors.
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

package org.springframework.kafka.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;

/**
 * An admin that delegates to an {@link AdminClient} to create topics defined
 * in the application context.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3
 */
public class KafkaAdmin extends KafkaResourceFactory
		implements ApplicationContextAware, SmartInitializingSingleton, KafkaAdminOperations {

	/**
	 * The default close timeout duration as 10 seconds.
	 */
	public static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(10);

	private static final int DEFAULT_OPERATION_TIMEOUT = 30;

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(KafkaAdmin.class));

	private final Map<String, Object> configs;

	private ApplicationContext applicationContext;

	private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;

	private int operationTimeout = DEFAULT_OPERATION_TIMEOUT;

	private boolean fatalIfBrokerNotAvailable;

	private boolean autoCreate = true;

	private boolean initializingContext;

	/**
	 * Create an instance with an {@link AdminClient} based on the supplied
	 * configuration.
	 * @param config the configuration for the {@link AdminClient}.
	 */
	public KafkaAdmin(Map<String, Object> config) {
		this.configs = new HashMap<>(config);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	/**
	 * Set the close timeout in seconds. Defaults to {@link #DEFAULT_CLOSE_TIMEOUT} seconds.
	 * @param closeTimeout the timeout.
	 */
	public void setCloseTimeout(int closeTimeout) {
		this.closeTimeout = Duration.ofSeconds(closeTimeout);
	}

	/**
	 * Set the operation timeout in seconds. Defaults to {@value #DEFAULT_OPERATION_TIMEOUT} seconds.
	 * @param operationTimeout the timeout.
	 */
	public void setOperationTimeout(int operationTimeout) {
		this.operationTimeout = operationTimeout;
	}

	/**
	 * Set to true if you want the application context to fail to load if we are unable
	 * to connect to the broker during initialization, to check/add topics.
	 * @param fatalIfBrokerNotAvailable true to fail.
	 */
	public void setFatalIfBrokerNotAvailable(boolean fatalIfBrokerNotAvailable) {
		this.fatalIfBrokerNotAvailable = fatalIfBrokerNotAvailable;
	}

	/**
	 * Set to false to suppress auto creation of topics during context initialization.
	 * @param autoCreate boolean flag to indicate creating topics or not during context initialization
	 * @see #initialize()
	 */
	public void setAutoCreate(boolean autoCreate) {
		this.autoCreate = autoCreate;
	}

	@Override
	public Map<String, Object> getConfigurationProperties() {
		Map<String, Object> configs2 = new HashMap<>(this.configs);
		checkBootstrap(configs2);
		return Collections.unmodifiableMap(configs2);
	}

	@Override
	public void afterSingletonsInstantiated() {
		this.initializingContext = true;
		if (this.autoCreate) {
			initialize();
		}
	}

	/**
	 * Call this method to check/add topics; this might be needed if the broker was not
	 * available when the application context was initialized, and
	 * {@link #setFatalIfBrokerNotAvailable(boolean) fatalIfBrokerNotAvailable} is false,
	 * or {@link #setAutoCreate(boolean) autoCreate} was set to false.
	 * @return true if successful.
	 * @see #setFatalIfBrokerNotAvailable(boolean)
	 * @see #setAutoCreate(boolean)
	 */
	public final boolean initialize() {
		Collection<NewTopic> newTopics = new ArrayList<>(
				this.applicationContext.getBeansOfType(NewTopic.class, false, false).values());
		Collection<NewTopics> wrappers = this.applicationContext.getBeansOfType(NewTopics.class, false, false).values();
		wrappers.forEach(wrapper -> newTopics.addAll(wrapper.getNewTopics()));
		if (newTopics.size() > 0) {
			AdminClient adminClient = null;
			try {
				adminClient = createAdmin();
			}
			catch (Exception e) {
				if (!this.initializingContext || this.fatalIfBrokerNotAvailable) {
					throw new IllegalStateException("Could not create admin", e);
				}
				else {
					LOGGER.error(e, "Could not create admin");
				}
			}
			if (adminClient != null) {
				try {
					addOrModifyTopicsIfNeeded(adminClient, newTopics);
					return true;
				}
				catch (Exception e) {
					if (!this.initializingContext || this.fatalIfBrokerNotAvailable) {
						throw new IllegalStateException("Could not configure topics", e);
					}
					else {
						LOGGER.error(e, "Could not configure topics");
					}
				}
				finally {
					this.initializingContext = false;
					adminClient.close(this.closeTimeout);
				}
			}
		}
		this.initializingContext = false;
		return false;
	}

	@Override
	public void createOrModifyTopics(NewTopic... topics) {
		try (AdminClient client = createAdmin()) {
			addOrModifyTopicsIfNeeded(client, Arrays.asList(topics));
		}
	}

	@Override
	public Map<String, TopicDescription> describeTopics(String... topicNames) {
		try (AdminClient admin = createAdmin()) {
			Map<String, TopicDescription> results = new HashMap<>();
			DescribeTopicsResult topics = admin.describeTopics(Arrays.asList(topicNames));
			try {
				results.putAll(topics.all().get(this.operationTimeout, TimeUnit.SECONDS));
				return results;
			}
			catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				throw new KafkaException("Interrupted while getting topic descriptions", ie);
			}
			catch (TimeoutException | ExecutionException ex) {
				throw new KafkaException("Failed to obtain topic descriptions", ex);
			}
		}
	}

	private AdminClient createAdmin() {
		Map<String, Object> configs2 = new HashMap<>(this.configs);
		checkBootstrap(configs2);
		return AdminClient.create(configs2);
	}

	private void addOrModifyTopicsIfNeeded(AdminClient adminClient, Collection<NewTopic> topics) {
		if (topics.size() > 0) {
			Map<String, NewTopic> topicNameToTopic = new HashMap<>();
			topics.forEach(t -> topicNameToTopic.compute(t.name(), (k, v) -> t));
			DescribeTopicsResult topicInfo = adminClient
					.describeTopics(topics.stream()
							.map(NewTopic::name)
							.collect(Collectors.toList()));
			List<NewTopic> topicsToAdd = new ArrayList<>();
			Map<String, NewPartitions> topicsToModify = checkPartitions(topicNameToTopic, topicInfo, topicsToAdd);
			if (topicsToAdd.size() > 0) {
				addTopics(adminClient, topicsToAdd);
			}
			if (topicsToModify.size() > 0) {
				modifyTopics(adminClient, topicsToModify);
			}
		}
	}

	private Map<String, NewPartitions> checkPartitions(Map<String, NewTopic> topicNameToTopic,
			DescribeTopicsResult topicInfo, List<NewTopic> topicsToAdd) {

		Map<String, NewPartitions> topicsToModify = new HashMap<>();
		topicInfo.values().forEach((n, f) -> {
			NewTopic topic = topicNameToTopic.get(n);
			try {
				TopicDescription topicDescription = f.get(this.operationTimeout, TimeUnit.SECONDS);
				if (topic.numPartitions() >= 0 && topic.numPartitions() < topicDescription.partitions().size()) {
					LOGGER.info(() -> String.format(
						"Topic '%s' exists but has a different partition count: %d not %d", n,
						topicDescription.partitions().size(), topic.numPartitions()));
				}
				else if (topic.numPartitions() > topicDescription.partitions().size()) {
					LOGGER.info(() -> String.format(
						"Topic '%s' exists but has a different partition count: %d not %d, increasing "
						+ "if the broker supports it", n,
						topicDescription.partitions().size(), topic.numPartitions()));
					topicsToModify.put(n, NewPartitions.increaseTo(topic.numPartitions()));
				}
			}
			catch (@SuppressWarnings("unused") InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			catch (TimeoutException e) {
				throw new KafkaException("Timed out waiting to get existing topics", e);
			}
			catch (@SuppressWarnings("unused") ExecutionException e) {
				topicsToAdd.add(topic);
			}
		});
		return topicsToModify;
	}

	private void addTopics(AdminClient adminClient, List<NewTopic> topicsToAdd) {
		CreateTopicsResult topicResults = adminClient.createTopics(topicsToAdd);
		try {
			topicResults.all().get(this.operationTimeout, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.error(e, "Interrupted while waiting for topic creation results");
		}
		catch (TimeoutException e) {
			throw new KafkaException("Timed out waiting for create topics results", e);
		}
		catch (ExecutionException e) {
			if (e.getCause() instanceof TopicExistsException) { // Possible race with another app instance
				LOGGER.debug(e.getCause(), "Failed to create topics");
			}
			else {
				LOGGER.error(e.getCause(), "Failed to create topics");
				throw new KafkaException("Failed to create topics", e.getCause()); // NOSONAR
			}
		}
	}

	private void modifyTopics(AdminClient adminClient, Map<String, NewPartitions> topicsToModify) {
		CreatePartitionsResult partitionsResult = adminClient.createPartitions(topicsToModify);
		try {
			partitionsResult.all().get(this.operationTimeout, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.error(e, "Interrupted while waiting for partition creation results");
		}
		catch (TimeoutException e) {
			throw new KafkaException("Timed out waiting for create partitions results", e);
		}
		catch (ExecutionException e) {
			if (e.getCause() instanceof InvalidPartitionsException) { // Possible race with another app instance
				LOGGER.debug(e.getCause(), "Failed to create partitions");
			}
			else {
				LOGGER.error(e.getCause(), "Failed to create partitions");
				if (!(e.getCause() instanceof UnsupportedVersionException)) {
					throw new KafkaException("Failed to create partitions", e.getCause()); // NOSONAR
				}
			}
		}
	}

	/**
	 * Wrapper for a collection of {@link NewTopic} to facilitated declaring multiple
	 * topics as as single bean.
	 *
	 * @since 2.7
	 *
	 */
	public static class NewTopics {

		final Collection<NewTopic> newTopics = new ArrayList<>();

		/**
		 * Construct an instance with the {@link NewTopic}s.
		 * @param newTopics the topics.
		 */
		public NewTopics(NewTopic... newTopics) {
			this.newTopics.addAll(Arrays.asList(newTopics));
		}

		Collection<NewTopic> getNewTopics() {
			return this.newTopics;
		}

	}

}
