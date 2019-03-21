/*
 * Copyright 2015-2018 the original author or authors.
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

package org.springframework.kafka.test.rule;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
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

import javax.net.ServerSocketFactory;

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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
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

import kafka.admin.AdminUtils$;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.server.NotRunning;
import kafka.utils.CoreUtils;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import scala.collection.JavaConversions;
import scala.collection.Set;

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

	private final String clientVersion;

	private final int count;

	private final boolean controlledShutdown;

	private final String[] topics;

	private final int partitionsPerTopic;

	private final List<KafkaServer> kafkaServers = new ArrayList<>();

	private EmbeddedZookeeper zookeeper;

	private ZkClient zookeeperClient;

	private String zkConnect;

	private Map<String, String> brokerProperties;

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
	 *
	 * Create embedded Kafka brokers.
	 * @param count the number of brokers.
	 * @param controlledShutdown passed into TestUtils.createBrokerConfig.
	 * @param partitions partitions per topic.
	 * @param topics the topics to create.
	 */
	public KafkaEmbedded(int count, boolean controlledShutdown, int partitions, String... topics) {
		this.clientVersion = AppInfoParser.getVersion();
		this.count = count;
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
		this.brokerProperties = brokerProperties;
		return this;
	}

	/**
	 * Set explicit ports on which the kafka brokers will listen. Useful when running an
	 * embedded broker that you want to access from other processes.
	 * @param kafkaPorts the ports.
	 * @since 1.3
	 */
	public void setKafkaPorts(int... kafkaPorts) {
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
			Integer port = this.kafkaPorts != null && this.kafkaPorts.length > i ? this.kafkaPorts[i] : null;
			if (port == null) {
				ServerSocket ss = ServerSocketFactory.getDefault().createServerSocket(0);
				port = ss.getLocalPort();
				ss.close();
			}
			Properties brokerConfigProperties = createProperties(i, port);
			brokerConfigProperties.setProperty(KafkaConfig$.MODULE$.PortProp(), "" + port);
			brokerConfigProperties.setProperty("replica.socket.timeout.ms", "1000");
			brokerConfigProperties.setProperty("controller.socket.timeout.ms", "1000");
			brokerConfigProperties.setProperty("offsets.topic.replication.factor", "1");
			if (this.brokerProperties != null) {
				this.brokerProperties.forEach(brokerConfigProperties::setProperty);
			}
			KafkaServer server = TestUtils.createServer(new KafkaConfig(brokerConfigProperties), Time.SYSTEM);
			this.kafkaServers.add(server);
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

	public Properties createProperties(int i, Integer port) {
		if (this.clientVersion.startsWith("0.11")) {
			return TestUtils.createBrokerConfig(i, this.zkConnect, this.controlledShutdown,
					true, port,
					scala.Option.apply(null),
					scala.Option.apply(null),
					scala.Option.apply(null),
					true, false, 0, false, 0, false, 0, scala.Option.apply(null));
		}
		else {
			try {
				Method method = TestUtils.class.getDeclaredMethod("createBrokerConfig",
						int.class, String.class, boolean.class, boolean.class, int.class,
						scala.Option.class, scala.Option.class, scala.Option.class,
						boolean.class, boolean.class, int.class, boolean.class, int.class, boolean.class,
						int.class, scala.Option.class, int.class);
				return (Properties) method.invoke(null, i, this.zkConnect, this.controlledShutdown,
					true, port,
					scala.Option.apply(null),
					scala.Option.apply(null),
					scala.Option.apply(null),
					true, false, 0, false, 0, false, 0, scala.Option.apply(null), 1);
			}
			catch (NoSuchMethodException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
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
		for (KafkaServer kafkaServer : this.kafkaServers) {
			addresses.add(new BrokerAddress("127.0.0.1", kafkaServer.config().port()));
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
		if (!this.clientVersion.startsWith("0.11")) {
			throw new UnsupportedOperationException("Not supported on clients greater than 0.11");
		}
		this.kafkaServers.get(index).shutdown();
		if (waitForPropagation) {
			long initialTime = System.currentTimeMillis();
			boolean canExit = false;
			do {
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException e) {
					break;
				}
				canExit = true;
				ZkUtils zkUtils = new ZkUtils(getZkClient(), null, false);
				scala.collection.Map<String, Properties> topicProperties =
						AdminUtils$.MODULE$.fetchAllTopicConfigs(zkUtils);
				Set<MetadataResponse.TopicMetadata> topicMetadatas =
						AdminUtils$.MODULE$.fetchTopicMetadataFromZk(topicProperties.keySet(), zkUtils);
				for (MetadataResponse.TopicMetadata topicMetadata : JavaConversions.asJavaCollection(topicMetadatas)) {
					if (Errors.forCode(topicMetadata.error().code()).exception() == null) {
						for (MetadataResponse.PartitionMetadata partitionMetadata : topicMetadata.partitionMetadata()) {
							Collection<Node> inSyncReplicas = partitionMetadata.isr();
							for (Node node : inSyncReplicas) {
								if (node.id() == index) {
									canExit = false;
								}
							}
						}
					}
				}
			}
			while (!canExit && (System.currentTimeMillis() - initialTime < METADATA_PROPAGATION_TIMEOUT));
		}

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
		if (!this.clientVersion.startsWith("0.11")) {
			throw new UnsupportedOperationException("Not supported on clients greater than 0.11");
		}
		long initialTime = System.currentTimeMillis();
		boolean canExit = false;
		do {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				break;
			}
			canExit = true;
			ZkUtils zkUtils = new ZkUtils(getZkClient(), null, false);
			MetadataResponse.TopicMetadata topicMetadata = AdminUtils$.MODULE$.fetchTopicMetadataFromZk(topic, zkUtils);
			if (Errors.forCode(topicMetadata.error().code()).exception() == null) {
				for (MetadataResponse.PartitionMetadata partitionMetadata : topicMetadata.partitionMetadata()) {
					Collection<Node> isr = partitionMetadata.isr();
					boolean containsIndex = false;
					for (Node node : isr) {
						if (node.id() == brokerId) {
							containsIndex = true;
						}
					}
					if (!containsIndex) {
						canExit = false;
					}

				}
			}
		}
		while (!canExit && (System.currentTimeMillis() - initialTime < METADATA_PROPAGATION_TIMEOUT));
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
		consumer.poll(0); // force assignment
		assertThat(consumerLatch.await(30, TimeUnit.SECONDS))
				.as("Failed to be assigned partitions from the embedded topics")
				.isTrue();
		logger.debug("Subscription Initiated");
	}

}
