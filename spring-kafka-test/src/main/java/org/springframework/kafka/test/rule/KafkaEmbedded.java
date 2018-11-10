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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.core.BrokerAddress;

import kafka.server.KafkaServer;
import kafka.zk.EmbeddedZookeeper;

/**
 * The {@link KafkaRule} implementation for the embedded Kafka Broker and Zookeeper.
 *
 * @author Marius Bogoevici
 * @author Artem Bilan
 * @author Gary Russell
 * @author Kamill Sokol
 * @author Elliot Kennedy
 * @author Nakul Mishra
 *
 * @deprecated since 2.2 in favor of {@link EmbeddedKafkaRule}
 */
@Deprecated
public class KafkaEmbedded extends EmbeddedKafkaRule implements KafkaRule, InitializingBean, DisposableBean {

	public static final String BEAN_NAME = "kafkaEmbedded";

	public static final String SPRING_EMBEDDED_KAFKA_BROKERS = EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS;

	public static final String SPRING_EMBEDDED_ZOOKEEPER_CONNECT = EmbeddedKafkaBroker.SPRING_EMBEDDED_ZOOKEEPER_CONNECT;

	public static final long METADATA_PROPAGATION_TIMEOUT = 10000L;


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
		super(count, controlledShutdown, partitions, topics);
	}

	/**
	 * Specify the properties to configure Kafka Broker before start, e.g.
	 * {@code auto.create.topics.enable}, {@code transaction.state.log.replication.factor} etc.
	 * @param brokerProperties the properties to use for configuring Kafka Broker(s).
	 * @return this for chaining configuration
	 * @see kafka.server.KafkaConfig
	 */
	@Override
	public KafkaEmbedded brokerProperties(Map<String, String> brokerProperties) {
		super.brokerProperties(brokerProperties);
		return this;
	}

	/**
	 * Specify a broker property.
	 * @param property the property name.
	 * @param value the value.
	 * @return the {@link KafkaEmbedded}.
	 * @since 2.1.4
	 */
	@Override
	public KafkaEmbedded brokerProperty(String property, Object value) {
		super.brokerProperty(property, value);
		return this;
	}

	/**
	 * Set explicit ports on which the kafka brokers will listen. Useful when running an
	 * embedded broker that you want to access from other processes.
	 * @param kafkaPorts the ports.
	 * @since 1.3
	 */
	public void setKafkaPorts(int... kafkaPorts) {
		super.kafkaPorts(kafkaPorts);
	}

	@Override
	public void afterPropertiesSet() {
		before();
	}

	/**
	 * Create an {@link AdminClient}; invoke the callback and reliably close the
	 * admin.
	 * @param callback the callback.
	 * @since 2.1.6
	 */
	public void doWithAdmin(java.util.function.Consumer<AdminClient> callback) {
		getEmbeddedKafka().doWithAdmin(callback);
	}

	public Properties createBrokerProperties(int i) {
		throw new UnsupportedOperationException();
	}


	@Override
	public void destroy() {
		after();
	}

	public Set<String> getTopics() {
		return getEmbeddedKafka().getTopics();
	}

	@Override
	public List<KafkaServer> getKafkaServers() {
		return getEmbeddedKafka().getKafkaServers();
	}

	public KafkaServer getKafkaServer(int id) {
		return getEmbeddedKafka().getKafkaServer(id);
	}

	public EmbeddedZookeeper getZookeeper() {
		return getEmbeddedKafka().getZookeeper();
	}

	@Override
	public ZkClient getZkClient() {
		return getEmbeddedKafka().getZkClient();
	}

	@Override
	public String getZookeeperConnectionString() {
		return getEmbeddedKafka().getZookeeperConnectionString();
	}

	public BrokerAddress getBrokerAddress(int i) {
		return getEmbeddedKafka().getBrokerAddress(i);
	}

	@Override
	public BrokerAddress[] getBrokerAddresses() {
		return getEmbeddedKafka().getBrokerAddresses();
	}

	@Override
	public int getPartitionsPerTopic() {
		return getEmbeddedKafka().getPartitionsPerTopic();
	}

	public void bounce(BrokerAddress brokerAddress) {
		getEmbeddedKafka().bounce(brokerAddress);
	}

	public void startZookeeper() {

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
		getEmbeddedKafka().restart(index);
	}

	@Deprecated
	public void waitUntilSynced(String topic, int brokerId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getBrokersAsString() {
		return getEmbeddedKafka().getBrokersAsString();
	}

	@Override
	public boolean isEmbedded() {
		return true;
	}

	/**
	 * Subscribe a consumer to all the embedded topics.
	 * @param consumer the consumer.
	 */
	public void consumeFromAllEmbeddedTopics(Consumer<?, ?> consumer) {
		getEmbeddedKafka().consumeFromAllEmbeddedTopics(consumer);
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
	 * @param topics the topics.
	 */
	public void consumeFromEmbeddedTopics(Consumer<?, ?> consumer, String... topics) {
		getEmbeddedKafka().consumeFromEmbeddedTopics(consumer, topics);
	}

}
