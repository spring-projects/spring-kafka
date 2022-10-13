/*
 * Copyright 2017-2022 the original author or authors.
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.support.TopicForRetryable;
import org.springframework.lang.Nullable;

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

	private boolean modifyTopicConfigs;

	private String clusterId;

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

	/**
	 * Set to true to compare the current topic configuration properties with those in the
	 * {@link NewTopic} bean, and update if different.
	 * @param modifyTopicConfigs true to check and update configs if necessary.
	 * @since 2.8.7
	 */
	public void setModifyTopicConfigs(boolean modifyTopicConfigs) {
		this.modifyTopicConfigs = modifyTopicConfigs;
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
		Collection<NewTopic> newTopics = newTopics();
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
					synchronized (this) {
						this.clusterId = adminClient.describeCluster().clusterId().get(this.operationTimeout,
								TimeUnit.SECONDS);
					}
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

	/*
	 * Remove any TopicForRetryable bean if there is also a NewTopic with the same topic name.
	 */
	private Collection<NewTopic> newTopics() {
		Map<String, NewTopic> newTopicsMap = new HashMap<>(
				this.applicationContext.getBeansOfType(NewTopic.class, false, false));
		Map<String, NewTopics> wrappers = this.applicationContext.getBeansOfType(NewTopics.class, false, false);
		AtomicInteger count = new AtomicInteger();
		wrappers.forEach((name, newTopics) -> {
			newTopics.getNewTopics().forEach(nt -> newTopicsMap.put(name + "#" + count.getAndIncrement(), nt));
		});
		Map<String, NewTopic> topicsForRetry = newTopicsMap.entrySet().stream()
				.filter(entry -> entry.getValue() instanceof TopicForRetryable)
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
		for (Entry<String, NewTopic> entry : topicsForRetry.entrySet()) {
			Iterator<Entry<String, NewTopic>> iterator = newTopicsMap.entrySet().iterator();
			boolean remove = false;
			while (iterator.hasNext()) {
				Entry<String, NewTopic> nt = iterator.next();
				// if we have a NewTopic and TopicForRetry with the same name, remove the latter
				if (nt.getValue().name().equals(entry.getValue().name())
						&& !(nt.getValue() instanceof TopicForRetryable)) {

					remove = true;
					break;
				}
			}
			if (remove) {
				newTopicsMap.remove(entry.getKey());
			}
		}
		Collection<NewTopic> newTopics = new ArrayList<>(newTopicsMap.values());
		return newTopics;
	}

	@Override
	@Nullable
	public String clusterId() {
		if (this.clusterId == null) {
			try (AdminClient client = createAdmin()) {
				this.clusterId = client.describeCluster().clusterId().get(this.operationTimeout, TimeUnit.SECONDS);
			}
			catch (Exception ex) {
				LOGGER.error(ex, "Could not obtaine cluster info");
			}
		}
		return this.clusterId;
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
				results.putAll(topics.allTopicNames().get(this.operationTimeout, TimeUnit.SECONDS));
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
			Map<String, NewPartitions> topicsWithPartitionMismatches =
					checkPartitions(topicNameToTopic, topicInfo, topicsToAdd);
			if (topicsToAdd.size() > 0) {
				addTopics(adminClient, topicsToAdd);
			}
			if (topicsWithPartitionMismatches.size() > 0) {
				createMissingPartitions(adminClient, topicsWithPartitionMismatches);
			}
			if (this.modifyTopicConfigs) {
				List<NewTopic> toCheck = new LinkedList<>(topics);
				toCheck.removeAll(topicsToAdd);
				Map<ConfigResource, List<ConfigEntry>> mismatchingConfigs =
						checkTopicsForConfigMismatches(adminClient, toCheck);
				if (!mismatchingConfigs.isEmpty()) {
					adjustConfigMismatches(adminClient, topics, mismatchingConfigs);
				}
			}
		}
	}

	private Map<ConfigResource, List<ConfigEntry>> checkTopicsForConfigMismatches(
			AdminClient adminClient, Collection<NewTopic> topics) {

		List<ConfigResource> configResources = topics.stream()
				.map(topic -> new ConfigResource(Type.TOPIC, topic.name()))
				.collect(Collectors.toList());

		DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(configResources);
		try {
			Map<ConfigResource, Config> topicsConfig = describeConfigsResult.all()
					.get(this.operationTimeout, TimeUnit.SECONDS);

			Map<ConfigResource, List<ConfigEntry>> configMismatches = new HashMap<>();
			for (Map.Entry<ConfigResource, Config> topicConfig : topicsConfig.entrySet()) {
				Optional<NewTopic> topicOptional = topics.stream()
						.filter(p -> p.name().equals(topicConfig.getKey().name()))
						.findFirst();

				List<ConfigEntry> configMismatchesEntries = new ArrayList<>();
				if (topicOptional.isPresent() && topicOptional.get().configs() != null) {
					for (Map.Entry<String, String> desiredConfigParameter : topicOptional.get().configs().entrySet()) {
						ConfigEntry actualConfigParameter = topicConfig.getValue().get(desiredConfigParameter.getKey());
						if (!desiredConfigParameter.getValue().equals(actualConfigParameter.value())) {
							configMismatchesEntries.add(actualConfigParameter);
						}
					}
					if (configMismatchesEntries.size() > 0) {
						configMismatches.put(topicConfig.getKey(), configMismatchesEntries);
					}
				}
			}
			return configMismatches;
		}
		catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new KafkaException("Interrupted while getting topic descriptions:" + topics, ie);
		}
		catch (ExecutionException | TimeoutException ex) {
			throw new KafkaException("Failed to obtain topic descriptions:" + topics, ex);
		}
	}

	private void adjustConfigMismatches(AdminClient adminClient, Collection<NewTopic> topics,
			Map<ConfigResource, List<ConfigEntry>> mismatchingConfigs) {
		for (Map.Entry<ConfigResource, List<ConfigEntry>> mismatchingConfigsOfTopic : mismatchingConfigs.entrySet()) {
			ConfigResource topicConfigResource = mismatchingConfigsOfTopic.getKey();

			Optional<NewTopic> topicOptional = topics.stream().filter(p -> p.name().equals(topicConfigResource.name()))
					.findFirst();
			if (topicOptional.isPresent()) {
				for (ConfigEntry mismatchConfigEntry : mismatchingConfigsOfTopic.getValue()) {
					List<AlterConfigOp> alterConfigOperations = new ArrayList<>();
					Map<String, String> desiredConfigs = topicOptional.get().configs();
					if (desiredConfigs.get(mismatchConfigEntry.name()) != null) {
						alterConfigOperations.add(
								new AlterConfigOp(
										new ConfigEntry(mismatchConfigEntry.name(),
												desiredConfigs.get(mismatchConfigEntry.name())),
										OpType.SET));
					}
					if (alterConfigOperations.size() > 0) {
						try {
							AlterConfigsResult alterConfigsResult = adminClient
									.incrementalAlterConfigs(Map.of(topicConfigResource, alterConfigOperations));
							alterConfigsResult.all().get(this.operationTimeout, TimeUnit.SECONDS);
						}
						catch (InterruptedException ie) {
							Thread.currentThread().interrupt();
							throw new KafkaException("Interrupted while getting topic descriptions", ie);
						}
						catch (ExecutionException | TimeoutException ex) {
							throw new KafkaException("Failed to obtain topic descriptions", ex);
						}
					}
				}
			}

		}
	}

	private Map<String, NewPartitions> checkPartitions(Map<String, NewTopic> topicNameToTopic,
			DescribeTopicsResult topicInfo, List<NewTopic> topicsToAdd) {

		Map<String, NewPartitions> topicsToModify = new HashMap<>();
		topicInfo.topicNameValues().forEach((n, f) -> {
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

	private void createMissingPartitions(AdminClient adminClient, Map<String, NewPartitions> topicsToModify) {
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
	 * Wrapper for a collection of {@link NewTopic} to facilitate declaring multiple
	 * topics as a single bean.
	 *
	 * @since 2.7
	 *
	 */
	public static class NewTopics {

		private final Collection<NewTopic> newTopics = new ArrayList<>();

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
