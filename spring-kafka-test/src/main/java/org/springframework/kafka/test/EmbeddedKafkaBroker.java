/*
 * Copyright 2023-present the original author or authors.
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

import java.util.Map;
import java.util.Set;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * @author Gary Russell
 * @since 3.1
 *
 */
public interface EmbeddedKafkaBroker extends InitializingBean, DisposableBean {

	int DEFAULT_ADMIN_TIMEOUT = 10;

	/**
	 * Set explicit ports on which the kafka brokers will listen. Useful when running an
	 * embedded broker that you want to access from other processes.
	 * @param ports the ports.
	 * @return the {@link EmbeddedKafkaBroker}.
	 */
	EmbeddedKafkaBroker kafkaPorts(int... ports);

	String BEAN_NAME = "embeddedKafka";

	/**
	 * Set the value of this property to a property name that should be set to the list of
	 * embedded broker addresses instead of {@value #SPRING_EMBEDDED_KAFKA_BROKERS}.
	 */
	String BROKER_LIST_PROPERTY = "spring.embedded.kafka.brokers.property";

	String SPRING_EMBEDDED_KAFKA_BROKERS = "spring.embedded.kafka.brokers";

	String BROKER_NEEDED = "Broker must be started before this method can be called";

	String LOOPBACK = "127.0.0.1";

	/**
	 * Get the topics.
	 * @return the topics.
	 */
	Set<String> getTopics();

	@Override
	default void destroy() {
	}

	@Override
	default void afterPropertiesSet() {
	}

	/**
	 * Specify the properties to configure Kafka Broker before start, e.g.
	 * {@code auto.create.topics.enable}, {@code transaction.state.log.replication.factor} etc.
	 * @param properties the properties to use for configuring Kafka Broker(s).
	 * @return this for chaining configuration.
	 * @see KafkaConfig
	 */
	EmbeddedKafkaBroker brokerProperties(Map<String, String> properties);

	/**
	 * Set the system property with this name to the list of broker addresses.
	 * Defaults to {@code spring.kafka.bootstrap-servers} for Spring Boot
	 * compatibility.
	 * @param brokerListProperty the brokerListProperty to set
	 * @return this broker.
	 */
	EmbeddedKafkaBroker brokerListProperty(String brokerListProperty);

	/**
	 * Set the timeout in seconds for admin operations (e.g. topic creation, close).
	 * @param adminTimeout the timeout.
	 * @return the {@link EmbeddedKafkaBroker}
	 * @since 2.8.5
	 */
	EmbeddedKafkaBroker adminTimeout(int adminTimeout);

	/**
	 * Get the bootstrap server addresses as a String.
	 * @return the bootstrap servers.
	 */
	String getBrokersAsString();

	/**
	 * Add topics to the existing broker(s) using the configured number of partitions.
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 */
	void addTopics(String... topicsToAdd);

	/**
	 * Add topics to the existing broker(s).
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 */
	void addTopics(NewTopic... topicsToAdd);

	/**
	 * Add topics to the existing broker(s) and returning a map of results.
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 * @return the results; null values indicate success.
	 */
	Map<String, Exception> addTopicsWithResults(NewTopic... topicsToAdd);

	/**
	 * Add topics to the existing broker(s) using the configured number of partitions.
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 * @return the results; null values indicate success.
	 */
	Map<String, Exception> addTopicsWithResults(String... topicsToAdd);

	/**
	 * Subscribe a consumer to one or more of the embedded topics.
	 * @param consumer the consumer.
	 * @param topicsToConsume the topics.
	 * @param seekToEnd true to seek to the end instead of the beginning.
	 * @throws IllegalStateException if you attempt to consume from a topic that is not in
	 * the list of embedded topics.
	 */
	void consumeFromEmbeddedTopics(Consumer<?, ?> consumer, boolean seekToEnd, String... topicsToConsume);

	/**
	 * Subscribe a consumer to one or more of the embedded topics.
	 * @param consumer the consumer.
	 * @param topicsToConsume the topics.
	 * @throws IllegalStateException if you attempt to consume from a topic that is not in
	 * the list of embedded topics.
	 */
	void consumeFromEmbeddedTopics(Consumer<?, ?> consumer, String... topicsToConsume);

	/**
	 * Subscribe a consumer to one of the embedded topics.
	 * @param consumer the consumer.
	 * @param seekToEnd true to seek to the end instead of the beginning.
	 * @param topic the topic.
	 */
	void consumeFromAnEmbeddedTopic(Consumer<?, ?> consumer, boolean seekToEnd, String topic);

	/**
	 * Subscribe a consumer to one of the embedded topics.
	 * @param consumer the consumer.
	 * @param topic the topic.
	 */
	void consumeFromAnEmbeddedTopic(Consumer<?, ?> consumer, String topic);

	/**
	 * Subscribe a consumer to all the embedded topics.
	 * @param seekToEnd true to seek to the end instead of the beginning.
	 * @param consumer the consumer.
	 */
	void consumeFromAllEmbeddedTopics(Consumer<?, ?> consumer, boolean seekToEnd);

	/**
	 * Subscribe a consumer to all the embedded topics.
	 * @param consumer the consumer.
	 */
	void consumeFromAllEmbeddedTopics(Consumer<?, ?> consumer);

	/**
	 * Get the configured number of partitions per topic.
	 * @return the partition count.
	 */
	int getPartitionsPerTopic();

}
