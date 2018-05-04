/*
 * Copyright 2015-2018 the original author or authors.
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

package org.springframework.kafka.test.rule;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Time;
import org.junit.rules.ExternalResource;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.kafka.test.core.BrokerAddress;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.NotRunning;
import kafka.utils.CoreUtils;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;

/**
 * The {@link KafkaRule} implementation for the embedded Kafka Broker and Zookeeper.
 *
 * @author Marius Bogoevici
 * @author Artem Bilan
 * @author Gary Russell
 * @author Kamill Sokol
 * @author Elliot Kennedy
 */
public class KafkaEmbedded extends ExternalResource implements KafkaRule, InitializingBean, DisposableBean {

	private static final Log logger = LogFactory.getLog(KafkaEmbedded.class);

	public static final String BEAN_NAME = "kafkaEmbedded";

	public static final String SPRING_EMBEDDED_KAFKA_BROKERS = "spring.embedded.kafka.brokers";

	public static final String SPRING_EMBEDDED_ZOOKEEPER_CONNECT = "spring.embedded.zookeeper.connect";

	public static final long METADATA_PROPAGATION_TIMEOUT = 10000L;

	private static final String clientVersion;

	private static final Method testUtilsCreateBrokerConfigMethod;

	static {
		clientVersion = AppInfoParser.getVersion();
		if (clientVersion.startsWith("1.1.")) {
			try {
				testUtilsCreateBrokerConfigMethod = TestUtils.class.getDeclaredMethod("createBrokerConfig",
						int.class, String.class, boolean.class, boolean.class, int.class,
						scala.Option.class, scala.Option.class, scala.Option.class,
						boolean.class, boolean.class, int.class, boolean.class, int.class, boolean.class,
						int.class, scala.Option.class, int.class, boolean.class);
			}
			catch (NoSuchMethodException | SecurityException e) {
				throw new RuntimeException("Failed to determine TestUtils.createBrokerConfig() method, "
						+ "possible mismatched versions of the client/broker jars; see the appendix in "
						+ "the reference manual when overriding kafka-clients to version " + clientVersion);
			}
		}
		else {
			testUtilsCreateBrokerConfigMethod = null;
		}
	}

	private final int count;

	private final boolean controlledShutdown;

	private final String[] topics;

	private final int partitionsPerTopic;

	private final List<KafkaServer> kafkaServers = new ArrayList<>();

	private final Map<String, Object> brokerProperties = new HashMap<>();

	private EmbeddedZookeeper zookeeper;

	private ZkClient zookeeperClient;

	private String zkConnect;

	private int[] kafkaPorts;

	public KafkaEmbedded(int count) {
		this(count, false);
	}

	/**
	 * Create embedded Kafka brokers.
	 * @param count the number of brokers.
	 * @param controlledShutdown passed into TestUtils.createBrokerConfig.
	 * @param topics the topics to create (2 partitions per).
	 */
	public KafkaEmbedded(int count, boolean controlledShutdown, String... topics) {
		this(count, controlledShutdown, 2, topics);
	}

	/**
	 * Create embedded Kafka brokers listening on random ports.
	 * @param count the number of brokers.
	 * @param controlledShutdown passed into TestUtils.createBrokerConfig.
	 * @param partitions partitions per topic.
	 * @param topics the topics to create.
	 */
	public KafkaEmbedded(int count, boolean controlledShutdown, int partitions, String... topics) {
		this.count = count;
		this.kafkaPorts = new int[this.count]; // random ports by default.
		this.controlledShutdown = controlledShutdown;
		if (topics != null) {
			this.topics = topics;
		}
		else {
			this.topics = new String[0];
		}
		this.partitionsPerTopic = partitions;
	}

	/**
	 * Specify the properties to configure Kafka Broker before start, e.g.
	 * {@code auto.create.topics.enable}, {@code transaction.state.log.replication.factor} etc.
	 * @param brokerProperties the properties to use for configuring Kafka Broker(s).
	 * @return this for chaining configuration
	 * @see KafkaConfig
	 */
	public KafkaEmbedded brokerProperties(Map<String, String> brokerProperties) {
		this.brokerProperties.putAll(brokerProperties);
		return this;
	}

	/**
	 * Specify a broker property.
	 * @param property the property name.
	 * @param value the value.
	 * @return the {@link KafkaEmbedded}.
	 * @since 2.1.4
	 */
	public KafkaEmbedded brokerProperty(String property, Object value) {
		this.brokerProperties.put(property, value);
		return this;
	}

	/**
	 * Set explicit ports on which the kafka brokers will listen. Useful when running an
	 * embedded broker that you want to access from other processes.
	 * @param kafkaPorts the ports.
	 * @since 1.3
	 */
	public void setKafkaPorts(int... kafkaPorts) {
		Assert.isTrue(kafkaPorts.length == this.count, "A port must be provided for each instance ["
				+ this.count + "], provided: " + Arrays.toString(kafkaPorts) + ", use 0 for a random port");
		this.kafkaPorts = kafkaPorts;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		before();
	}

	@Override
	public void before() throws Exception { //NOSONAR
		startZookeeper();
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
			if (this.brokerProperties != null) {
				this.brokerProperties.forEach(brokerConfigProperties::put);
			}
			KafkaServer server = TestUtils.createServer(new KafkaConfig(brokerConfigProperties), Time.SYSTEM);
			this.kafkaServers.add(server);
			if (this.kafkaPorts[i] == 0) {
				this.kafkaPorts[i] = TestUtils.boundPort(server, SecurityProtocol.PLAINTEXT);
			}
		}
		Map<String, Object> adminConfigs = new HashMap<>();
		adminConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokersAsString());
		AdminClient admin = AdminClient.create(adminConfigs);
		List<NewTopic> newTopics = Arrays.stream(this.topics)
				.map(t -> new NewTopic(t, this.partitionsPerTopic, (short) this.count))
				.collect(Collectors.toList());
		CreateTopicsResult createTopics = admin.createTopics(newTopics);
		createTopics.all().get();
		admin.close();
		System.setProperty(SPRING_EMBEDDED_KAFKA_BROKERS, getBrokersAsString());
		System.setProperty(SPRING_EMBEDDED_ZOOKEEPER_CONNECT, getZookeeperConnectionString());
	}

	public Properties createBrokerProperties(int i) {
		if (testUtilsCreateBrokerConfigMethod == null) {
			return TestUtils.createBrokerConfig(i, this.zkConnect, this.controlledShutdown,
					true, this.kafkaPorts[i],
					scala.Option.apply(null),
					scala.Option.apply(null),
					scala.Option.apply(null),
					true, false, 0, false, 0, false, 0, scala.Option.apply(null), 1);
		}
		else {
			try {
				return (Properties) testUtilsCreateBrokerConfigMethod.invoke(null, i, this.zkConnect,
					this.controlledShutdown,
					true, this.kafkaPorts[i],
					scala.Option.<SecurityProtocol>apply(null),
					scala.Option.<File>apply(null),
					scala.Option.<Properties>apply(null),
					true, false, 0, false, 0, false, 0, scala.Option.<String>apply(null), 1, false);
			}
			catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
	}


	@Override
	public void destroy() throws Exception {
		after();
	}

	@Override
	public void after() {
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

	@Override
	public List<KafkaServer> getKafkaServers() {
		return this.kafkaServers;
	}

	public KafkaServer getKafkaServer(int id) {
		return this.kafkaServers.get(id);
	}

	public EmbeddedZookeeper getZookeeper() {
		return this.zookeeper;
	}

	@Override
	public ZkClient getZkClient() {
		return this.zookeeperClient;
	}

	@Override
	public String getZookeeperConnectionString() {
		return this.zkConnect;
	}

	public BrokerAddress getBrokerAddress(int i) {
		KafkaServer kafkaServer = this.kafkaServers.get(i);
		return new BrokerAddress("127.0.0.1", kafkaServer.config().port());
	}

	@Override
	public BrokerAddress[] getBrokerAddresses() {
		List<BrokerAddress> addresses = new ArrayList<BrokerAddress>();
		for (int i = 0; i < this.kafkaPorts.length; i++) {
			addresses.add(new BrokerAddress("127.0.0.1", this.kafkaPorts[i]));
		}
		return addresses.toArray(new BrokerAddress[addresses.size()]);
	}

	@Override
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

	public void startZookeeper() {
		this.zookeeper = new EmbeddedZookeeper();
	}

	@Deprecated
	public void bounce(int index, boolean waitForPropagation) {
		throw new UnsupportedOperationException();
	}

	@Deprecated
	public void bounce(int index) {
		bounce(index, true);
	}

	public void restart(final int index) throws Exception { //NOSONAR

		// retry restarting repeatedly, first attempts may fail

		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(10,
				Collections.<Class<? extends Throwable>, Boolean>singletonMap(Exception.class, true));

		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(100);
		backOffPolicy.setMaxInterval(1000);
		backOffPolicy.setMultiplier(2);

		RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(retryPolicy);
		retryTemplate.setBackOffPolicy(backOffPolicy);


		retryTemplate.execute(new RetryCallback<Void, Exception>() {

			@Override
			public Void doWithRetry(RetryContext context) throws Exception { //NOSONAR
				KafkaEmbedded.this.kafkaServers.get(index).startup();
				return null;
			}
		});
	}

	@Deprecated
	public void waitUntilSynced(String topic, int brokerId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getBrokersAsString() {
		StringBuilder builder = new StringBuilder();
		for (BrokerAddress brokerAddress : getBrokerAddresses()) {
			builder.append(brokerAddress.toString()).append(',');
		}
		return builder.substring(0, builder.length() - 1);
	}

	@Override
	public boolean isEmbedded() {
		return true;
	}

	/**
	 * Subscribe a consumer to all the embedded topics.
	 * @param consumer the consumer.
	 * @throws Exception an exception.
	 */
	public void consumeFromAllEmbeddedTopics(Consumer<?, ?> consumer) throws Exception {
		consumeFromEmbeddedTopics(consumer, this.topics);
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
		for (String topic : topics) {
			assertThat(this.topics).as("topic '" + topic + "' is not in embedded topic list").contains(topic);
		}
		final CountDownLatch consumerLatch = new CountDownLatch(1);
		consumer.subscribe(Arrays.asList(topics), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				consumerLatch.countDown();
				if (logger.isDebugEnabled()) {
					logger.debug("partitions assigned: " + partitions);
				}
			}

		});
		ConsumerRecords<?, ?> records = consumer.poll(0); // force assignment
		if (records.count() > 0) {
			if (logger.isDebugEnabled()) {
				logger.debug("Records received on initial poll for assignment; re-seeking to beginning; "
						+ records.partitions().stream()
							.flatMap(p -> records.records(p).stream())
							// map to same format as send metadata toString()
							.map(r -> r.topic() + "-" + r.partition() + "@" + r.offset())
							.collect(Collectors.toList()));
			}
			consumer.seekToBeginning(records.partitions());
		}
		assertThat(consumerLatch.await(30, TimeUnit.SECONDS))
				.as("Failed to be assigned partitions from the embedded topics")
				.isTrue();
		logger.debug("Subscription Initiated");
	}

}
