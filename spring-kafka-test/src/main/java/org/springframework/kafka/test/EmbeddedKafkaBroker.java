/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.kafka.test;

import static org.assertj.core.api.Assertions.assertThat;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.commons.logging.Log;
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

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
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
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;

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

	private static final Log logger = LogFactory.getLog(EmbeddedKafkaBroker.class);

	public static final String BEAN_NAME = "embeddedKafka";

	public static final String SPRING_EMBEDDED_KAFKA_BROKERS = "spring.embedded.kafka.brokers";

	public static final String SPRING_EMBEDDED_ZOOKEEPER_CONNECT = "spring.embedded.zookeeper.connect";

	private final int count;

	private final boolean controlledShutdown;

	private final Set<String> topics;

	private final int partitionsPerTopic;

	private final List<KafkaServer> kafkaServers = new ArrayList<>();

	private final Map<String, Object> brokerProperties = new HashMap<>();

	private EmbeddedZookeeper zookeeper;

	private ZkClient zookeeperClient;

	private String zkConnect;

	private int[] kafkaPorts;

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
	 * @param brokerProperties the properties to use for configuring Kafka Broker(s).
	 * @return this for chaining configuration.
	 * @see KafkaConfig
	 */
	public EmbeddedKafkaBroker brokerProperties(Map<String, String> brokerProperties) {
		this.brokerProperties.putAll(brokerProperties);
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
	 * @param kafkaPorts the ports.
	 * @return the {@link EmbeddedKafkaBroker}.
	 */
	public EmbeddedKafkaBroker kafkaPorts(int... kafkaPorts) {
		Assert.isTrue(kafkaPorts.length == this.count, "A port must be provided for each instance ["
				+ this.count + "], provided: " + Arrays.toString(kafkaPorts) + ", use 0 for a random port");
		this.kafkaPorts = kafkaPorts;
		return this;
	}

	@Override
	public void afterPropertiesSet() {
		this.zookeeper = new EmbeddedZookeeper();
		int zkConnectionTimeout = 6000;
		int zkSessionTimeout = 6000;

		this.zkConnect = "127.0.0.1:" + this.zookeeper.port();
		this.zookeeperClient = new ZkClient(this.zkConnect, zkSessionTimeout, zkConnectionTimeout,
				ZKStringSerializer$.MODULE$);
		this.kafkaServers.clear();
		for (int i = 0; i < this.count; i++) {
			Properties brokerConfigProperties = createBrokerProperties(i);
			brokerConfigProperties.setProperty(KafkaConfig.ReplicaSocketTimeoutMsProp(), "1000");
			brokerConfigProperties.setProperty(KafkaConfig.ControllerSocketTimeoutMsProp(), "1000");
			brokerConfigProperties.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
			brokerConfigProperties.setProperty(KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp(),
					String.valueOf(Long.MAX_VALUE));
			if (this.brokerProperties != null) {
				this.brokerProperties.forEach(brokerConfigProperties::put);
			}
			KafkaServer server = TestUtils.createServer(new KafkaConfig(brokerConfigProperties), Time.SYSTEM);
			this.kafkaServers.add(server);
			if (this.kafkaPorts[i] == 0) {
				this.kafkaPorts[i] = TestUtils.boundPort(server, SecurityProtocol.PLAINTEXT);
			}
		}
		createKafkaTopics(this.topics);
		System.setProperty(SPRING_EMBEDDED_KAFKA_BROKERS, getBrokersAsString());
		System.setProperty(SPRING_EMBEDDED_ZOOKEEPER_CONNECT, getZookeeperConnectionString());
	}

	private Properties createBrokerProperties(int i) {
		return TestUtils.createBrokerConfig(i, this.zkConnect, this.controlledShutdown,
				true, this.kafkaPorts[i],
				scala.Option.apply(null),
				scala.Option.apply(null),
				scala.Option.apply(null),
				true, false, 0, false, 0, false, 0, scala.Option.apply(null), 1, false);
	}

	/**
	 * Create topics in the existing broker(s) using the configured number of partitions.
	 * @param topics the topics.
	 */
	private void createKafkaTopics(Set<String> topics) {
		doWithAdmin(admin -> {
			List<NewTopic> newTopics = topics.stream()
					.map(t -> new NewTopic(t, this.partitionsPerTopic, (short) this.count))
					.collect(Collectors.toList());
			CreateTopicsResult createTopics = admin.createTopics(newTopics);
			try {
				createTopics.all().get();
			}
			catch (Exception e) {
				throw new KafkaException(e);
			}
		});
	}


	/**
	 * Add topics to the existing broker(s) using the configured number of partitions.
	 * @param topics the topics.
	 */
	public void addTopics(String... topics) {
		HashSet<String> set = new HashSet<>(Arrays.asList(topics));
		createKafkaTopics(set);
		this.topics.addAll(set);
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

	@Override
	public void destroy() {
		System.getProperties().remove(SPRING_EMBEDDED_KAFKA_BROKERS);
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
		try {
			this.zookeeperClient.close();
		}
		catch (ZkInterruptedException e) {
			// do nothing
		}
		try {
			this.zookeeper.shutdown();
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

	public ZkClient getZkClient() {
		return this.zookeeperClient;
	}

	public String getZookeeperConnectionString() {
		return this.zkConnect;
	}

	public BrokerAddress getBrokerAddress(int i) {
		KafkaServer kafkaServer = this.kafkaServers.get(i);
		return new BrokerAddress("127.0.0.1", kafkaServer.config().port());
	}

	public BrokerAddress[] getBrokerAddresses() {
		List<BrokerAddress> addresses = new ArrayList<BrokerAddress>();
		for (int i = 0; i < this.kafkaPorts.length; i++) {
			addresses.add(new BrokerAddress("127.0.0.1", this.kafkaPorts[i]));
		}
		return addresses.toArray(new BrokerAddress[addresses.size()]);
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

		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(10, Collections.singletonMap(Exception.class, true));

		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(100);
		backOffPolicy.setMaxInterval(1000);
		backOffPolicy.setMultiplier(2);

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
	 * @throws Exception an exception.
	 */
	public void consumeFromAllEmbeddedTopics(Consumer<?, ?> consumer) throws Exception {
		consumeFromEmbeddedTopics(consumer, this.topics.toArray(new String[0]));
	}

	/**
	 * Subscribe a consumer to one of the embedded topics.
	 * @param consumer the consumer.
	 * @param topic the topic.
	 * @throws Exception an exception.
	 */
	public void consumeFromAnEmbeddedTopic(Consumer<?, ?> consumer, String topic) throws Exception {
		consumeFromEmbeddedTopics(consumer, topic);
	}

	/**
	 * Subscribe a consumer to one or more of the embedded topics.
	 * @param consumer the consumer.
	 * @param topics the topics.
	 * @throws Exception an exception.
	 */
	public void consumeFromEmbeddedTopics(Consumer<?, ?> consumer, String... topics) throws Exception {
		HashSet<String> diff = new HashSet<>(Arrays.asList(topics));
		diff.removeAll(new HashSet<>(this.topics));
		assertThat(this.topics)
				.as("topic(s):'" + diff + "' are not in embedded topic list")
				.containsAll(new HashSet<>(Arrays.asList(topics)));
		final AtomicBoolean assigned = new AtomicBoolean();
		consumer.subscribe(Arrays.asList(topics), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				assigned.set(true);
				if (logger.isDebugEnabled()) {
					logger.debug("partitions assigned: " + partitions);
				}
			}

		});
		ConsumerRecords<?, ?> records = null;
		int n = 0;
		while (!assigned.get() && n++ < 600) {
			records = consumer.poll(Duration.ofMillis(100)); // force assignment
		}
		if (records != null && records.count() > 0) {
			final ConsumerRecords<?, ?> theRecords = records;
			if (logger.isDebugEnabled()) {
				logger.debug("Records received on initial poll for assignment; re-seeking to beginning; "
						+ records.partitions().stream()
						.flatMap(p -> theRecords.records(p).stream())
						// map to same format as send metadata toString()
						.map(r -> r.topic() + "-" + r.partition() + "@" + r.offset())
						.collect(Collectors.toList()));
			}
			consumer.seekToBeginning(records.partitions());
		}
		assertThat(assigned.get())
				.as("Failed to be assigned partitions from the embedded topics")
				.isTrue();
		logger.debug("Subscription Initiated");
	}

}
