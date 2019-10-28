/*
 * Copyright 2018-2019 the original author or authors.
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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.test.core.BrokerAddress;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

import kafka.common.KafkaException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.NotRunning;
import kafka.utils.CoreUtils;
import kafka.utils.TestUtils;
import kafka.zk.ZkFourLetterWords;
import kafka.zookeeper.ZooKeeperClient;

/**
 * An embedded Kafka Broker(s) and Zookeeper manager.
 * This class is intended to be used in the unit tests.
 *
 * @author Marius Bogoevici
 * @author Artem Bilan
 * @author Gary Russell
 * @author Kamill Sokol
 * @author Elliot Kennedy
 * @author Nakul Mishra
 *
 * @since 2.2
 */
public class EmbeddedKafkaBroker implements InitializingBean, DisposableBean {

	private static final String LOOPBACK = "127.0.0.1";

	private static final LogAccessor logger = new LogAccessor(LogFactory.getLog(EmbeddedKafkaBroker.class)); // NOSONAR

	public static final String BEAN_NAME = "embeddedKafka";

	public static final String SPRING_EMBEDDED_KAFKA_BROKERS = "spring.embedded.kafka.brokers";

	public static final String SPRING_EMBEDDED_ZOOKEEPER_CONNECT = "spring.embedded.zookeeper.connect";

	/**
	 * Set the value of this property to a property name that should be set to the list of
	 * embedded broker addresses instead of {@value #SPRING_EMBEDDED_KAFKA_BROKERS}.
	 */
	public static final String BROKER_LIST_PROPERTY = "spring.embedded.kafka.brokers.property";

	private static final Duration DEFAULT_ADMIN_TIMEOUT = Duration.ofSeconds(10);

	private static final int ZK_CONNECTION_TIMEOUT = 6000;

	private static final int ZK_SESSION_TIMEOUT = 6000;

	private final int count;

	private final boolean controlledShutdown;

	private final Set<String> topics;

	private final int partitionsPerTopic;

	private final List<KafkaServer> kafkaServers = new ArrayList<>();

	private final Map<String, Object> brokerProperties = new HashMap<>();

	private EmbeddedZookeeper zookeeper;

	private String zkConnect;

	private int zkPort;

	private int[] kafkaPorts;

	private Duration adminTimeout = DEFAULT_ADMIN_TIMEOUT;

	private String brokerListProperty;

	private volatile ZooKeeperClient zooKeeperClient;

	public EmbeddedKafkaBroker(int count) {
		this(count, false);
	}

	/**
	 * Create embedded Kafka brokers.
	 * @param count the number of brokers.
	 * @param controlledShutdown passed into TestUtils.createBrokerConfig.
	 * @param topics the topics to create (2 partitions per).
	 */
	public EmbeddedKafkaBroker(int count, boolean controlledShutdown, String... topics) {
		this(count, controlledShutdown, 2, topics);
	}

	/**
	 * Create embedded Kafka brokers listening on random ports.
	 * @param count the number of brokers.
	 * @param controlledShutdown passed into TestUtils.createBrokerConfig.
	 * @param partitions partitions per topic.
	 * @param topics the topics to create.
	 */
	public EmbeddedKafkaBroker(int count, boolean controlledShutdown, int partitions, String... topics) {
		this.count = count;
		this.kafkaPorts = new int[this.count]; // random ports by default.
		this.controlledShutdown = controlledShutdown;
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
	public EmbeddedKafkaBroker brokerProperties(Map<String, String> properties) {
		this.brokerProperties.putAll(properties);
		return this;
	}

	/**
	 * Specify a broker property.
	 * @param property the property name.
	 * @param value the value.
	 * @return the {@link EmbeddedKafkaBroker}.
	 */
	public EmbeddedKafkaBroker brokerProperty(String property, Object value) {
		this.brokerProperties.put(property, value);
		return this;
	}

	/**
	 * Set explicit ports on which the kafka brokers will listen. Useful when running an
	 * embedded broker that you want to access from other processes.
	 * @param ports the ports.
	 * @return the {@link EmbeddedKafkaBroker}.
	 */
	public EmbeddedKafkaBroker kafkaPorts(int... ports) {
		Assert.isTrue(ports.length == this.count, "A port must be provided for each instance ["
				+ this.count + "], provided: " + Arrays.toString(ports) + ", use 0 for a random port");
		this.kafkaPorts = Arrays.copyOf(ports, ports.length);
		return this;
	}

	/**
	 * Set an explicit port for the embedded Zookeeper.
	 * @param port the port.
	 * @return the {@link EmbeddedKafkaBroker}.
	 * @since 2.3
	 */
	public EmbeddedKafkaBroker zkPort(int port) {
		this.zkPort = port;
		return this;
	}
	/**
	 * Set the timeout in seconds for admin operations (e.g. topic creation, close).
	 * Default 30 seconds.
	 * @param adminTimeout the timeout.
	 * @since 2.2
	 */
	public void setAdminTimeout(int adminTimeout) {
		this.adminTimeout = Duration.ofSeconds(adminTimeout);
	}

	/**
	 * Set the system property with this name to the list of broker addresses.
	 * @param brokerListProperty the brokerListProperty to set
	 * @return this broker.
	 * @since 2.3
	 */
	public EmbeddedKafkaBroker brokerListProperty(String brokerListProperty) {
		this.brokerListProperty = brokerListProperty;
		return this;
	}

	/**
	 * Get the port that the embedded Zookeeper is running on or will run on.
	 * @return the port.
	 * @since 2.3
	 */
	public int getZkPort() {
		return this.zookeeper != null ? this.zookeeper.getPort() : this.zkPort;
	}

	/**
	 * Set the port to run the embedded Zookeeper on (default random).
	 * @param zkPort the port.
	 * @since 2.3
	 */
	public void setZkPort(int zkPort) {
		this.zkPort = zkPort;
	}

	@Override
	public void afterPropertiesSet() {
		try {
			this.zookeeper = new EmbeddedZookeeper(this.zkPort);
		}
		catch (IOException | InterruptedException e) {
			throw new IllegalStateException("Failed to create embedded Zookeeper", e);
		}
		this.zkConnect = LOOPBACK + ":" + this.zookeeper.getPort();
		this.kafkaServers.clear();
		for (int i = 0; i < this.count; i++) {
			Properties brokerConfigProperties = createBrokerProperties(i);
			brokerConfigProperties.setProperty(KafkaConfig.ReplicaSocketTimeoutMsProp(), "1000");
			brokerConfigProperties.setProperty(KafkaConfig.ControllerSocketTimeoutMsProp(), "1000");
			brokerConfigProperties.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
			brokerConfigProperties.setProperty(KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp(),
					String.valueOf(Long.MAX_VALUE));
			this.brokerProperties.forEach(brokerConfigProperties::put);
			if (!this.brokerProperties.containsKey(KafkaConfig.NumPartitionsProp())) {
				brokerConfigProperties.setProperty(KafkaConfig.NumPartitionsProp(), "" + this.partitionsPerTopic);
			}
			KafkaServer server = TestUtils.createServer(new KafkaConfig(brokerConfigProperties), Time.SYSTEM);
			this.kafkaServers.add(server);
			if (this.kafkaPorts[i] == 0) {
				this.kafkaPorts[i] = TestUtils.boundPort(server, SecurityProtocol.PLAINTEXT);
			}
		}
		createKafkaTopics(this.topics);
		if (this.brokerListProperty == null) {
			this.brokerListProperty = System.getProperty(BROKER_LIST_PROPERTY);
		}
		if (this.brokerListProperty == null) {
			this.brokerListProperty = SPRING_EMBEDDED_KAFKA_BROKERS;
		}
		System.setProperty(this.brokerListProperty, getBrokersAsString());
		System.setProperty(SPRING_EMBEDDED_ZOOKEEPER_CONNECT, getZookeeperConnectionString());
	}

	private Properties createBrokerProperties(int i) {
		return TestUtils.createBrokerConfig(i, this.zkConnect, this.controlledShutdown,
				true, this.kafkaPorts[i],
				scala.Option.apply(null),
				scala.Option.apply(null),
				scala.Option.apply(null),
				true, false, 0, false, 0, false, 0, scala.Option.apply(null), 1, false,
				this.partitionsPerTopic, (short) this.count);
	}

	/**
	 * Add topics to the existing broker(s) using the configured number of partitions.
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 */
	public void addTopics(String... topicsToAdd) {
		Assert.notNull(this.zookeeper, "Broker must be started before this method can be called");
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
	public void addTopics(NewTopic... topicsToAdd) {
		Assert.notNull(this.zookeeper, "Broker must be started before this method can be called");
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
	 * Create an {@link AdminClient}; invoke the callback and reliably close the admin.
	 * @param callback the callback.
	 */
	public void doWithAdmin(java.util.function.Consumer<AdminClient> callback) {
		Map<String, Object> adminConfigs = new HashMap<>();
		adminConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokersAsString());
		AdminClient admin = null;
		try {
			admin = AdminClient.create(adminConfigs);
			callback.accept(admin);
		}
		finally {
			if (admin != null) {
				admin.close(this.adminTimeout);
			}
		}
	}

	@Override
	public void destroy() {
		System.getProperties().remove(brokerListProperty);
		System.getProperties().remove(SPRING_EMBEDDED_ZOOKEEPER_CONNECT);
		for (KafkaServer kafkaServer : this.kafkaServers) {
			try {
				if (kafkaServer.brokerState().currentState() != (NotRunning.state())) {
					kafkaServer.shutdown();
					kafkaServer.awaitShutdown();
				}
			}
			catch (Exception e) {
				// do nothing
			}
			try {
				CoreUtils.delete(kafkaServer.config().logDirs());
			}
			catch (Exception e) {
				// do nothing
			}
		}
		synchronized (this) {
			if (this.zooKeeperClient != null) {
				this.zooKeeperClient.close();
			}
		}
		try {
			this.zookeeper.shutdown();
			this.zkConnect = null;
		}
		catch (Exception e) {
			// do nothing
		}
	}

	public Set<String> getTopics() {
		return new HashSet<>(this.topics);
	}

	public List<KafkaServer> getKafkaServers() {
		return this.kafkaServers;
	}

	public KafkaServer getKafkaServer(int id) {
		return this.kafkaServers.get(id);
	}

	public EmbeddedZookeeper getZookeeper() {
		return this.zookeeper;
	}

	/**
	 * Return the ZooKeeperClient.
	 * @return the client.
	 * @since 2.3.2
	 */
	public synchronized ZooKeeperClient getZooKeeperClient() {
		if (this.zooKeeperClient == null) {
			this.zooKeeperClient = new ZooKeeperClient(this.zkConnect, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT,
					1, Time.SYSTEM, "embeddedKafkaZK", "embeddedKafkaZK");
		}
		return this.zooKeeperClient;
	}

	public String getZookeeperConnectionString() {
		return this.zkConnect;
	}

	public BrokerAddress getBrokerAddress(int i) {
		KafkaServer kafkaServer = this.kafkaServers.get(i);
		return new BrokerAddress(LOOPBACK, kafkaServer.config().port());
	}

	public BrokerAddress[] getBrokerAddresses() {
		List<BrokerAddress> addresses = new ArrayList<BrokerAddress>();
		for (int kafkaPort : this.kafkaPorts) {
			addresses.add(new BrokerAddress(LOOPBACK, kafkaPort));
		}
		return addresses.toArray(new BrokerAddress[0]);
	}

	public int getPartitionsPerTopic() {
		return this.partitionsPerTopic;
	}

	public void bounce(BrokerAddress brokerAddress) {
		for (KafkaServer kafkaServer : getKafkaServers()) {
			if (brokerAddress.equals(new BrokerAddress(kafkaServer.config().hostName(), kafkaServer.config().port()))) {
				kafkaServer.shutdown();
				kafkaServer.awaitShutdown();
			}
		}
	}

	public void restart(final int index) throws Exception { //NOSONAR

		// retry restarting repeatedly, first attempts may fail

		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(10, // NOSONAR magic #
				Collections.singletonMap(Exception.class, true));

		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(100); // NOSONAR magic #
		backOffPolicy.setMaxInterval(1000); // NOSONAR magic #
		backOffPolicy.setMultiplier(2); // NOSONAR magic #

		RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(retryPolicy);
		retryTemplate.setBackOffPolicy(backOffPolicy);


		retryTemplate.execute(context -> {
			this.kafkaServers.get(index).startup();
			return null;
		});
	}

	public String getBrokersAsString() {
		StringBuilder builder = new StringBuilder();
		for (BrokerAddress brokerAddress : getBrokerAddresses()) {
			builder.append(brokerAddress.toString()).append(',');
		}
		return builder.substring(0, builder.length() - 1);
	}

	/**
	 * Subscribe a consumer to all the embedded topics.
	 * @param consumer the consumer.
	 */
	public void consumeFromAllEmbeddedTopics(Consumer<?, ?> consumer) {
		consumeFromEmbeddedTopics(consumer, this.topics.toArray(new String[0]));
	}

	/**
	 * Subscribe a consumer to one of the embedded topics.
	 * @param consumer the consumer.
	 * @param topic the topic.
	 */
	public void consumeFromAnEmbeddedTopic(Consumer<?, ?> consumer, String topic) {
		consumeFromEmbeddedTopics(consumer, topic);
	}

	/**
	 * Subscribe a consumer to one or more of the embedded topics.
	 * @param consumer the consumer.
	 * @param topicsToConsume the topics.
	 * @throws IllegalStateException if you attempt to consume from a topic that is not in
	 * the list of embedded topics (since 2.3.4).
	 */
	public void consumeFromEmbeddedTopics(Consumer<?, ?> consumer, String... topicsToConsume) {
		List<String> notEmbedded = Arrays.stream(topicsToConsume)
				.filter(topic -> !this.topics.contains(topic))
				.collect(Collectors.toList());
		if (notEmbedded.size() > 0) {
			throw new IllegalStateException("topic(s):'" + notEmbedded + "' are not in embedded topic list");
		}
		final AtomicBoolean assigned = new AtomicBoolean();
		consumer.subscribe(Arrays.asList(topicsToConsume), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				assigned.set(true);
				logger.debug(() -> "partitions assigned: " + partitions);
			}

		});
		ConsumerRecords<?, ?> records = null;
		int n = 0;
		while (!assigned.get() && n++ < 600) { // NOSONAR magic #
			records = consumer.poll(Duration.ofMillis(100)); // force assignment NOSONAR magic #
		}
		if (records != null && records.count() > 0) {
			final ConsumerRecords<?, ?> theRecords = records;
			logger.debug(() -> "Records received on initial poll for assignment; re-seeking to beginning; "
					+ theRecords.partitions().stream()
					.flatMap(p -> theRecords.records(p).stream())
					// map to same format as send metadata toString()
					.map(r -> r.topic() + "-" + r.partition() + "@" + r.offset())
					.collect(Collectors.toList()));
			consumer.seekToBeginning(records.partitions());
		}
		if (!assigned.get()) {
			throw new IllegalStateException("Failed to be assigned partitions from the embedded topics");
		}
		logger.debug("Subscription Initiated");
	}

	/**
	 * Ported from scala to allow setting the port.
	 *
	 * @author Gary Russell
	 * @since 2.3
	 */
	public static final class EmbeddedZookeeper {

		private static final int THREE_K = 3000;

		private static final int HUNDRED = 100;

		private static final int TICK_TIME = 800; // allow a maxSessionTimeout of 20 * 800ms = 16 secs

		private final NIOServerCnxnFactory factory;

		private final ZooKeeperServer zookeeper;

		private final int port;

		private final File snapshotDir;

		private final File logDir;

		public EmbeddedZookeeper(int zkPort) throws IOException, InterruptedException {
			this.snapshotDir = TestUtils.tempDir();
			this.logDir = TestUtils.tempDir();
			System.setProperty("zookeeper.forceSync", "no"); // disable fsync to ZK txn
																// log in tests to avoid
																// timeout
			this.zookeeper = new ZooKeeperServer(this.snapshotDir, this.logDir, TICK_TIME);
			this.factory = new NIOServerCnxnFactory();
			InetSocketAddress addr = new InetSocketAddress(LOOPBACK, zkPort == 0 ? TestUtils.RandomPort() : zkPort);
			this.factory.configure(addr, 0);
			this.factory.startup(zookeeper);
			this.port = zookeeper.getClientPort();
		}

		public int getPort() {
			return this.port;
		}

		public File getSnapshotDir() {
			return this.snapshotDir;
		}

		public File getLogDir() {
			return this.logDir;
		}

		public void shutdown() throws IOException {
			// Also shuts down ZooKeeperServer
			try {
				this.factory.shutdown();
			}
			catch (Exception e) {
				logger.error(e, "ZK shutdown failed");
			}

			int n = 0;
			while (n++ < HUNDRED) {
				try {
					ZkFourLetterWords.sendStat(LOOPBACK, this.port, THREE_K);
					Thread.sleep(HUNDRED);
				}
				catch (@SuppressWarnings("unused") Exception e) {
					break;
				}
			}
			if (n == HUNDRED) {
				logger.debug("Zookeeper failed to stop");
			}

			try {
				this.zookeeper.getZKDatabase().close();
			}
			catch (Exception e) {
				logger.error(e, "ZK db close failed");
			}

			Utils.delete(this.logDir);
			Utils.delete(this.snapshotDir);
		}

	}

}
