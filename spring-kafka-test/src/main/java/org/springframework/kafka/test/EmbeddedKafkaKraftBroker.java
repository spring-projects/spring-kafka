/*
 * Copyright 2018-present the original author or authors.
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

package org.springframework.kafka.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import kafka.server.KafkaConfig;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.jspecify.annotations.Nullable;

import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

/**
 * An embedded Kafka Broker(s) using KRaft.
 * This class is intended to be used in the unit tests.
 *
 * @author Marius Bogoevici
 * @author Artem Bilan
 * @author Gary Russell
 * @author Kamill Sokol
 * @author Elliot Kennedy
 * @author Nakul Mishra
 * @author Pawel Lozinski
 * @author Adrian Chlebosz
 * @author Soby Chacko
 * @author Sanghyeok An
 * @author Wouter Coekaerts
 *
 * @since 3.1
 */
public class EmbeddedKafkaKraftBroker implements EmbeddedKafkaBroker {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(EmbeddedKafkaKraftBroker.class));

	/**
	 * Set the value of this property to a property name that should be set to the list of
	 * embedded broker addresses instead of {@value #SPRING_EMBEDDED_KAFKA_BROKERS}.
	 */
	public static final String BROKER_LIST_PROPERTY = "spring.embedded.kafka.brokers.property";

	public static final int DEFAULT_ADMIN_TIMEOUT = 10;

	private static final boolean IS_KAFKA_39_OR_LATER = ClassUtils.isPresent(
			"org.apache.kafka.server.config.AbstractKafkaConfig", EmbeddedKafkaKraftBroker.class.getClassLoader());

	private static final @Nullable Method SET_CONFIG_METHOD;

	static {
		if (IS_KAFKA_39_OR_LATER) {
			SET_CONFIG_METHOD = ReflectionUtils.findMethod(
					KafkaClusterTestKit.Builder.class,
					"setConfigProp",
					String.class, Object.class);
		}
		else {
			SET_CONFIG_METHOD = null;
		}
	}

	private final int count;

	private final Set<String> topics;

	private final int partitionsPerTopic;

	private final Properties brokerProperties = new Properties();

	private final AtomicBoolean initialized = new AtomicBoolean();

	private @Nullable KafkaClusterTestKit cluster;

	private int[] kafkaPorts;

	private Duration adminTimeout = Duration.ofSeconds(DEFAULT_ADMIN_TIMEOUT);

	private String brokerListProperty = "spring.kafka.bootstrap-servers";

	/**
	 * Create embedded Kafka brokers listening on random ports.
	 * @param count the number of brokers.
	 * @param partitions partitions per topic.
	 * @param topics the topics to create.
	 */
	public EmbeddedKafkaKraftBroker(int count, int partitions, String @Nullable ... topics) {
		this.count = count;
		this.kafkaPorts = new int[this.count]; // random ports by default.
		if (topics != null) {
			this.topics = new HashSet<>(Arrays.asList(topics));
		}
		else {
			this.topics = new HashSet<>();
		}
		this.partitionsPerTopic = partitions;
	}

	/**
	 * Specify the properties to configure Kafka Broker before start, e.g.
	 * {@code auto.create.topics.enable}, {@code transaction.state.log.replication.factor} etc.
	 * @param properties the properties to use for configuring Kafka Broker(s).
	 * @return this for chaining configuration.
	 * @see KafkaConfig
	 */
	@Override
	public EmbeddedKafkaBroker brokerProperties(Map<String, String> properties) {
		this.brokerProperties.putAll(properties);
		return this;
	}

	/**
	 * Specify a broker property.
	 * @param property the property name.
	 * @param value the value.
	 * @return the {@link EmbeddedKafkaKraftBroker}.
	 */
	public EmbeddedKafkaBroker brokerProperty(String property, Object value) {
		this.brokerProperties.put(property, value);
		return this;
	}

	/**
	 * IMPORTANT: It is not possible to configure custom ports when using KRaft based EmbeddedKafka.
	 * The {@link KafkaClusterTestKit} does not support setting custom ports at the moment.
	 * Therefore, this property is out of use.
	 * Set explicit ports on which the kafka brokers will listen. Useful when running an
	 * embedded broker that you want to access from other processes.
	 * @param ports the ports.
	 * @return the {@link EmbeddedKafkaKraftBroker}.
	 */
	@Override
	public EmbeddedKafkaKraftBroker kafkaPorts(int... ports) {
		Assert.isTrue(ports.length == this.count, "A port must be provided for each instance ["
				+ this.count + "], provided: " + Arrays.toString(ports) + ", use 0 for a random port");
		this.kafkaPorts = Arrays.copyOf(ports, ports.length);
		return this;
	}

	/**
	 * Set the system property with this name to the list of broker addresses.
	 * @param brokerListProperty the brokerListProperty to set
	 * @return this broker.
	 * @since 2.3
	 */
	@Override
	public EmbeddedKafkaBroker brokerListProperty(String brokerListProperty) {
		this.brokerListProperty = brokerListProperty;
		return this;
	}

	@Override
	public EmbeddedKafkaBroker adminTimeout(int adminTimeout) {
		this.adminTimeout = Duration.ofSeconds(adminTimeout);
		return this;
	}

	/**
	 * Set the timeout in seconds for admin operations (e.g. topic creation, close).
	 * Default 10 seconds.
	 * @param adminTimeout the timeout.
	 * @since 2.2
	 */
	public void setAdminTimeout(int adminTimeout) {
		this.adminTimeout = Duration.ofSeconds(adminTimeout);
	}

	@Override
	public void afterPropertiesSet() {
		if (this.initialized.compareAndSet(false, true)) {
			overrideExitMethods();
			addDefaultBrokerPropsIfAbsent();
			start();
		}
	}

	private void start() {
		if (this.cluster != null) {
			return;
		}
		try {
			KafkaClusterTestKit.Builder clusterBuilder = new KafkaClusterTestKit.Builder(
					new TestKitNodes.Builder()
							.setCombined(true)
							.setNumBrokerNodes(this.count)
							.setNumControllerNodes(this.count)
							.build());
			this.brokerProperties.forEach((k, v) -> setConfigProperty(clusterBuilder, (String) k, v));
			this.cluster = clusterBuilder.build();
		}
		catch (Exception ex) {
			throw new IllegalStateException("Failed to create embedded cluster", ex);
		}

		try {
			this.cluster.format();
			this.cluster.startup();
			this.cluster.waitForReadyBrokers();
		}
		catch (Exception ex) {
			throw new IllegalStateException("Failed to start test Kafka cluster", ex);
		}

		createKafkaTopics(this.topics);
		System.setProperty(this.brokerListProperty, getBrokersAsString());
		System.setProperty(SPRING_EMBEDDED_KAFKA_BROKERS, getBrokersAsString());
	}

	private static void setConfigProperty(KafkaClusterTestKit.Builder clusterBuilder, String key, Object value) {
		if (IS_KAFKA_39_OR_LATER) {
			// For Kafka 3.9.0+: use reflection
			if (SET_CONFIG_METHOD != null) {
				ReflectionUtils.invokeMethod(SET_CONFIG_METHOD, clusterBuilder, key, value);
			}
		}
		else {
			// For Kafka 3.8.0: direct call
			clusterBuilder.setConfigProp(key, (String) value);
		}
	}

	@Override
	public void destroy() {
		AtomicReference<@Nullable Throwable> shutdownFailure = new AtomicReference<>();
		Utils.closeQuietly(cluster, "embedded Kafka cluster", shutdownFailure);
		if (shutdownFailure.get() != null) {
			throw new IllegalStateException("Failed to shut down embedded Kafka cluster", shutdownFailure.get());
		}
		this.cluster = null;
	}

	private void addDefaultBrokerPropsIfAbsent() {
		this.brokerProperties.putIfAbsent("delete.topic.enable", "true");
		this.brokerProperties.putIfAbsent("group.initial.rebalance.delay.ms", "0");
		this.brokerProperties.putIfAbsent("offsets.topic.replication.factor", "" + this.count);
		this.brokerProperties.putIfAbsent("num.partitions", "" + this.partitionsPerTopic);
	}

	private void logDir(Properties brokerConfigProperties) {
		try {
			brokerConfigProperties.put("log.dir",
					Files.createTempDirectory("spring.kafka." + UUID.randomUUID()).toString());
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void overrideExitMethods() {
		String exitMsg = "Exit.%s(%d, %s) called";
		Exit.setExitProcedure((statusCode, message) -> {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(new RuntimeException(), String.format(exitMsg, "exit", statusCode, message));
			}
			else {
				LOGGER.warn(String.format(exitMsg, "exit", statusCode, message));
			}
		});
		Exit.setHaltProcedure((statusCode, message) -> {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(new RuntimeException(), String.format(exitMsg, "halt", statusCode, message));
			}
			else {
				LOGGER.warn(String.format(exitMsg, "halt", statusCode, message));
			}
		});
	}

	/**
	 * Add topics to the existing broker(s) using the configured number of partitions.
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 */
	@Override
	public void addTopics(String... topicsToAdd) {
		Assert.notNull(this.cluster, BROKER_NEEDED);
		HashSet<String> set = new HashSet<>(Arrays.asList(topicsToAdd));
		createKafkaTopics(set);
		this.topics.addAll(set);
	}

	/**
	 * Add topics to the existing broker(s).
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 * @since 2.2
	 */
	@Override
	public void addTopics(NewTopic... topicsToAdd) {
		Assert.notNull(this.cluster, BROKER_NEEDED);
		for (NewTopic topic : topicsToAdd) {
			Assert.isTrue(this.topics.add(topic.name()), () -> "topic already exists: " + topic);
			Assert.isTrue(topic.replicationFactor() <= this.count
							&& (topic.replicasAssignments() == null
							|| topic.replicasAssignments().size() <= this.count),
					() -> "Embedded kafka does not support the requested replication factor: " + topic);
		}

		doWithAdmin(admin -> createTopics(admin, Arrays.asList(topicsToAdd)));
	}

	/**
	 * Create topics in the existing broker(s) using the configured number of partitions.
	 * @param topicsToCreate the topics.
	 */
	private void createKafkaTopics(Set<String> topicsToCreate) {
		doWithAdmin(admin -> {
			createTopics(admin,
					topicsToCreate.stream()
						.map(t -> new NewTopic(t, this.partitionsPerTopic, (short) this.count))
						.collect(Collectors.toList()));
		});
	}

	private void createTopics(AdminClient admin, List<NewTopic> newTopics) {
		CreateTopicsResult createTopics = admin.createTopics(newTopics);
		try {
			createTopics.all().get(this.adminTimeout.getSeconds(), TimeUnit.SECONDS);
		}
		catch (Exception e) {
			throw new KafkaException(e);
		}
	}

	/**
	 * Add topics to the existing broker(s) using the configured number of partitions.
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 * @return the results; null values indicate success.
	 * @since 2.5.4
	 */
	@Override
	public Map<String, Exception> addTopicsWithResults(String... topicsToAdd) {
		Assert.notNull(this.cluster, BROKER_NEEDED);
		HashSet<String> set = new HashSet<>(Arrays.asList(topicsToAdd));
		this.topics.addAll(set);
		return createKafkaTopicsWithResults(set);
	}

	/**
	 * Add topics to the existing broker(s) and returning a map of results.
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 * @return the results; null values indicate success.
	 * @since 2.5.4
	 */
	@Override
	public Map<String, Exception> addTopicsWithResults(NewTopic... topicsToAdd) {
		Assert.notNull(this.cluster, BROKER_NEEDED);
		for (NewTopic topic : topicsToAdd) {
			Assert.isTrue(this.topics.add(topic.name()), () -> "topic already exists: " + topic);
			Assert.isTrue(topic.replicationFactor() <= this.count
							&& (topic.replicasAssignments() == null
							|| topic.replicasAssignments().size() <= this.count),
					() -> "Embedded kafka does not support the requested replication factor: " + topic);
		}

		return doWithAdminFunction(admin -> createTopicsWithResults(admin, Arrays.asList(topicsToAdd)));
	}

	/**
	 * Create topics in the existing broker(s) using the configured number of partitions
	 * and returning a map of results.
	 * @param topicsToCreate the topics.
	 * @return the results; null values indicate success.
	 * @since 2.5.4
	 */
	private Map<String, Exception> createKafkaTopicsWithResults(Set<String> topicsToCreate) {
		return doWithAdminFunction(admin -> {
			return createTopicsWithResults(admin,
					topicsToCreate.stream()
						.map(t -> new NewTopic(t, this.partitionsPerTopic, (short) this.count))
						.collect(Collectors.toList()));
		});
	}

	private Map<String, Exception> createTopicsWithResults(AdminClient admin, List<NewTopic> newTopics) {
		CreateTopicsResult createTopics = admin.createTopics(newTopics);
		Map<String, Exception> results = new HashMap<>();
		createTopics.values()
				.entrySet()
				.stream()
				.map(entry -> {
					Exception result;
					try {
						entry.getValue().get(this.adminTimeout.getSeconds(), TimeUnit.SECONDS);
						result = null;
					}
					catch (InterruptedException | ExecutionException | TimeoutException e) {
						result = e;
					}
					return new SimpleEntry<>(entry.getKey(), result);
				})
				.forEach(entry -> results.put(entry.getKey(), entry.getValue()));
		return results;
	}

	/**
	 * Create an {@link AdminClient}; invoke the callback and reliably close the admin.
	 * @param callback the callback.
	 */
	public void doWithAdmin(java.util.function.Consumer<AdminClient> callback) {
		Map<String, Object> adminConfigs = new HashMap<>();
		adminConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokersAsString());
		try (AdminClient admin = AdminClient.create(adminConfigs)) {
			callback.accept(admin);
		}
	}

	/**
	 * Create an {@link AdminClient}; invoke the callback and reliably close the admin.
	 * @param callback the callback.
	 * @param <T> the function return type.
	 * @return a map of results.
	 * @since 2.5.4
	 */
	public <T> T doWithAdminFunction(Function<AdminClient, T> callback) {
		Map<String, Object> adminConfigs = new HashMap<>();
		adminConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokersAsString());
		try (AdminClient admin = AdminClient.create(adminConfigs)) {
			return callback.apply(admin);
		}
	}

	@Override
	public Set<String> getTopics() {
		return new HashSet<>(this.topics);
	}

	@Override
	public int getPartitionsPerTopic() {
		return this.partitionsPerTopic;
	}

	@Override
	public String getBrokersAsString() {
		Assert.notNull(this.cluster, "cluster cannot be null");
		String brokersString = (String) this.cluster.clientProperties().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
		return Objects.requireNonNull(brokersString);
	}

	public @Nullable KafkaClusterTestKit getCluster() {
		return this.cluster;
	}

	/**
	 * Subscribe a consumer to all the embedded topics.
	 * @param consumer the consumer.
	 */
	@Override
	public void consumeFromAllEmbeddedTopics(Consumer<?, ?> consumer) {
		consumeFromEmbeddedTopics(consumer, this.topics.toArray(new String[0]));
	}

	/**
	 * Subscribe a consumer to all the embedded topics.
	 * @param seekToEnd true to seek to the end instead of the beginning.
	 * @param consumer the consumer.
	 * @since 2.8.2
	 */
	@Override
	public void consumeFromAllEmbeddedTopics(Consumer<?, ?> consumer, boolean seekToEnd) {
		consumeFromEmbeddedTopics(consumer, seekToEnd, this.topics.toArray(new String[0]));
	}

	/**
	 * Subscribe a consumer to one of the embedded topics.
	 * @param consumer the consumer.
	 * @param topic the topic.
	 */
	@Override
	public void consumeFromAnEmbeddedTopic(Consumer<?, ?> consumer, String topic) {
		consumeFromEmbeddedTopics(consumer, topic);
	}

	/**
	 * Subscribe a consumer to one of the embedded topics.
	 * @param consumer the consumer.
	 * @param seekToEnd true to seek to the end instead of the beginning.
	 * @param topic the topic.
	 * @since 2.8.2
	 */
	@Override
	public void consumeFromAnEmbeddedTopic(Consumer<?, ?> consumer, boolean seekToEnd, String topic) {
		consumeFromEmbeddedTopics(consumer, seekToEnd, topic);
	}

	/**
	 * Subscribe a consumer to one or more of the embedded topics.
	 * @param consumer the consumer.
	 * @param topicsToConsume the topics.
	 * @throws IllegalStateException if you attempt to consume from a topic that is not in
	 * the list of embedded topics (since 2.3.4).
	 */
	@Override
	public void consumeFromEmbeddedTopics(Consumer<?, ?> consumer, String... topicsToConsume) {
		consumeFromEmbeddedTopics(consumer, false, topicsToConsume);
	}

	/**
	 * Subscribe a consumer to one or more of the embedded topics.
	 * @param consumer the consumer.
	 * @param topicsToConsume the topics.
	 * @param seekToEnd true to seek to the end instead of the beginning.
	 * @throws IllegalStateException if you attempt to consume from a topic that is not in
	 * the list of embedded topics.
	 * @since 2.8.2
	 */
	@Override
	public void consumeFromEmbeddedTopics(Consumer<?, ?> consumer, boolean seekToEnd, String... topicsToConsume) {
		List<String> notEmbedded = Arrays.stream(topicsToConsume)
				.filter(topic -> !this.topics.contains(topic))
				.toList();
		if (!notEmbedded.isEmpty()) {
			throw new IllegalStateException("topic(s):'" + notEmbedded + "' are not in embedded topic list");
		}
		final AtomicReference<@Nullable Collection<TopicPartition>> assigned = new AtomicReference<>();
		consumer.subscribe(Arrays.asList(topicsToConsume), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				assigned.set(partitions);
				LOGGER.debug(() -> "partitions assigned: " + partitions);
			}

		});
		int n = 0;
		while (assigned.get() == null && n++ < 600) { // NOSONAR magic #
			consumer.poll(Duration.ofMillis(100)); // force assignment NOSONAR magic #
		}
		Collection<TopicPartition> topicPartitions = assigned.get();
		if (topicPartitions != null) {
			LOGGER.debug(() -> "Partitions assigned "
					+ topicPartitions
					+ "; re-seeking to "
					+ (seekToEnd ? "end; " : "beginning"));
			if (seekToEnd) {
				consumer.seekToEnd(topicPartitions);
				// seekToEnd is asynchronous. query the position to force the seek to happen now.
				topicPartitions.forEach(consumer::position);
			}
			else {
				consumer.seekToBeginning(topicPartitions);
			}
		}
		else {
			throw new IllegalStateException("Failed to be assigned partitions from the embedded topics");
		}
		LOGGER.debug("Subscription Initiated");
	}

}
