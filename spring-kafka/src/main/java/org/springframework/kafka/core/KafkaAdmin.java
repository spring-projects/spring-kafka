/*
 * Copyright 2017-present the original author or authors.
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
import org.jspecify.annotations.Nullable;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.env.EnvironmentCapable;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.support.TopicForRetryable;
import org.springframework.util.Assert;

/**
 * An admin that delegates to an {@link Admin} to create topics defined
 * in the application context.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Adrian Gygax
 * @author Sanghyeok An
 * @author Valentina Armenise
 * @author Anders Swanson
 * @author Omer Celik
 * @author Choi Wang Gyu
 * @author Go Beom Jun
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

	private static final AtomicInteger CLIENT_ID_COUNTER = new AtomicInteger();

	private final Lock clusterIdLock = new ReentrantLock();

	private final Map<String, Object> configs;

	private @Nullable ApplicationContext applicationContext;

	private Predicate<NewTopic> createOrModifyTopic = nt -> true;

	private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;

	private int operationTimeout = DEFAULT_OPERATION_TIMEOUT;

	private boolean fatalIfBrokerNotAvailable;

	private boolean autoCreate = true;

	private boolean initializingContext;

	private boolean modifyTopicConfigs;

	private @Nullable String clusterId;

	/**
	 * Create an instance with an {@link Admin} based on the supplied
	 * configuration.
	 * @param config the configuration for the {@link Admin}.
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
	 * Return the operation timeout in seconds.
	 * @return the timeout.
	 * @since 3.0.2
	 */
	public int getOperationTimeout() {
		return this.operationTimeout;
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

	/**
	 * Set a predicate that returns true if a discovered {@link NewTopic} bean should be
	 * considered for creation or modification by this admin instance. The default
	 * predicate returns true for all {@link NewTopic}s. Used by the default
	 * implementation of {@link #newTopics()}.
	 * @param createOrModifyTopic the predicate.
	 * @since 2.9.10
	 * @see #newTopics()
	 */
	public void setCreateOrModifyTopic(Predicate<NewTopic> createOrModifyTopic) {
		Assert.notNull(createOrModifyTopic, "'createOrModifyTopic' cannot be null");
		this.createOrModifyTopic = createOrModifyTopic;
	}

	/**
	 * Return the predicate used to determine whether a {@link NewTopic} should be
	 * considered for creation or modification.
	 * @return the predicate.
	 * @since 2.9.10
	 * @see #newTopics()
	 */
	protected Predicate<NewTopic> getCreateOrModifyTopic() {
		return this.createOrModifyTopic;
	}

	/**
	 * Set the cluster id. Use this to prevent attempting to fetch the cluster id
	 * from the broker, perhaps if the user does not have admin permissions.
	 * @param clusterId the clusterId to set
	 * @since 3.1
	 */
	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	/**
	 * Get the clusterId property.
	 * @return the cluster id.
	 * @since 3.1.8
	 */
	public @Nullable String getClusterId() {
		return this.clusterId;
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
		if (!newTopics.isEmpty()) {
			Admin adminClient = null;
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
					updateClusterId(adminClient);
					addOrModifyTopicsIfNeeded(adminClient, newTopics);
					return true;
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
				catch (Exception ex) {
					if (!this.initializingContext || this.fatalIfBrokerNotAvailable) {
						throw new IllegalStateException("Could not configure topics", ex);
					}
					else {
						LOGGER.error(ex, "Could not configure topics");
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

	private void updateClusterId(Admin adminClient) throws InterruptedException, ExecutionException, TimeoutException {
		try {
			this.clusterIdLock.lock();
			if (this.clusterId == null) {
				this.clusterId = adminClient.describeCluster().clusterId().get(this.operationTimeout,
						TimeUnit.SECONDS);
			}
		}
		finally {
			this.clusterIdLock.unlock();
		}
	}

	/**
	 * Return a collection of {@link NewTopic}s to create or modify. The default
	 * implementation retrieves all {@link NewTopic} beans in the application context and
	 * applies the {@link #setCreateOrModifyTopic(Predicate)} predicate to each one. It
	 * also removes any {@link TopicForRetryable} bean if there is also a NewTopic with
	 * the same topic name. This is performed before calling the predicate.
	 * @return the collection of {@link NewTopic}s.
	 * @since 2.9.10
	 * @see #setCreateOrModifyTopic(Predicate)
	 */
	protected Collection<NewTopic> newTopics() {
		Assert.state(this.applicationContext != null, "'applicationContext' cannot be null");

		// Deal with List<NewTopic> directly instead of Map (no need for bean names)
		List<NewTopic> newTopicsList = new ArrayList<>(
				this.applicationContext.getBeansOfType(NewTopic.class, false, false).values());

		// Add topics from NewTopics wrappers (no need for bean names either)
		this.applicationContext.getBeansOfType(NewTopics.class, false, false).values()
				.forEach(wrapper -> newTopicsList.addAll(wrapper.getNewTopics()));

		// Collect regular topic names to check against TopicForRetryable
		Set<String> regularTopicNames = newTopicsList.stream()
				.filter(nt -> !(nt instanceof TopicForRetryable))
				.map(NewTopic::name)
				.collect(Collectors.toSet());

		// Apply combined filter: remove TopicForRetryable with same name as regular topic OR topics that don't pass predicate
		newTopicsList.removeIf(nt ->
				(nt instanceof TopicForRetryable && regularTopicNames.contains(nt.name())) ||
				!this.createOrModifyTopic.test(nt));

		return newTopicsList;
	}

	@Override
	@Nullable
	public String clusterId() {
		if (this.clusterId == null) {
			try (Admin client = createAdmin()) {
				this.clusterId = client.describeCluster().clusterId().get(this.operationTimeout, TimeUnit.SECONDS);
				if (this.clusterId == null) {
					this.clusterId = "null";
				}
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			catch (Exception ex) {
				LOGGER.error(ex, "Could not obtain cluster info");
			}
		}
		return this.clusterId;
	}

	@Override
	public void createOrModifyTopics(NewTopic... topics) {
		try (Admin client = createAdmin()) {
			addOrModifyTopicsIfNeeded(client, Arrays.asList(topics));
		}
	}

	@Override
	public Map<String, TopicDescription> describeTopics(String... topicNames) {
		try (Admin admin = createAdmin()) {
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

	/**
	 * Delete topics from the Kafka cluster.
	 * @param topicNames the topic names to delete.
	 * @throws KafkaException if the operation fails.
	 * @since 4.0
	 */
	@Override
	public void deleteTopics(String... topicNames) {
		if (topicNames.length == 0) {
			return;
		}
		try (Admin admin = createAdmin()) {
			admin.deleteTopics(Arrays.asList(topicNames))
					.all()
					.get(this.operationTimeout, TimeUnit.SECONDS);
			LOGGER.debug(() -> "Deleted topics: " + Arrays.toString(topicNames));
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new KafkaException("Interrupted while deleting topics", ex);
		}
		catch (TimeoutException ex) {
			throw new KafkaException("Timed out waiting to delete topics", ex);
		}
		catch (ExecutionException ex) {
			throw new KafkaException("Failed to delete topics", ex.getCause());
		}
	}

	/**
	 * Creates a new {@link Admin} client instance using the {@link AdminClient} class.
	 * @return the new {@link Admin} client instance.
	 * @since 3.3.0
	 * @see AdminClient#create(Map)
	 */
	protected Admin createAdmin() {
		return AdminClient.create(getAdminConfig());
	}

	protected Map<String, Object> getAdminConfig() {
		final Map<String, Object> configs2 = new HashMap<>(this.configs);
		checkBootstrap(configs2);

		if (!configs2.containsKey(AdminClientConfig.CLIENT_ID_CONFIG)) {
			Optional.ofNullable(this.applicationContext)
					.map(EnvironmentCapable::getEnvironment)
					.map(environment -> environment.getProperty("spring.application.name"))
					.ifPresent(applicationName -> configs2.put(
							AdminClientConfig.CLIENT_ID_CONFIG,
							applicationName + "-admin-" + CLIENT_ID_COUNTER.getAndIncrement())
					);
		}
		return configs2;
	}

	private void addOrModifyTopicsIfNeeded(Admin adminClient, Collection<NewTopic> topics) {
		if (!topics.isEmpty()) {
			Map<String, NewTopic> topicNameToTopic = new HashMap<>();
			topics.forEach(t -> topicNameToTopic.compute(t.name(), (k, v) -> t));
			DescribeTopicsResult topicInfo = adminClient
					.describeTopics(topics.stream()
							.map(NewTopic::name)
							.collect(Collectors.toList()));
			List<NewTopic> topicsToAdd = new ArrayList<>();
			Map<String, NewPartitions> topicsWithPartitionMismatches =
					checkPartitions(topicNameToTopic, topicInfo, topicsToAdd);
			if (!topicsToAdd.isEmpty()) {
				addTopics(adminClient, topicsToAdd);
			}
			if (!topicsWithPartitionMismatches.isEmpty()) {
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
			Admin adminClient, Collection<NewTopic> topics) {

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
						if (actualConfigParameter == null) {
							throw new IllegalStateException("Topic property '" + desiredConfigParameter.getKey()
									+ "' does not exist");
						}
						if (!desiredConfigParameter.getValue().equals(actualConfigParameter.value())) {
							configMismatchesEntries.add(actualConfigParameter);
						}
					}
					if (!configMismatchesEntries.isEmpty()) {
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

	private void adjustConfigMismatches(Admin adminClient, Collection<NewTopic> topics,
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
					if (!alterConfigOperations.isEmpty()) {
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
				if (topic != null) {
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

	private void addTopics(Admin adminClient, List<NewTopic> topicsToAdd) {
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
				LOGGER.error(e.getCause() != null ? e.getCause() : e, "Failed to create topics");
				throw new KafkaException("Failed to create topics", e.getCause()); // NOSONAR
			}
		}
	}

	private void createMissingPartitions(Admin adminClient, Map<String, NewPartitions> topicsToModify) {
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
				LOGGER.error(e.getCause() != null ? e.getCause() : e, "Failed to create partitions");
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
