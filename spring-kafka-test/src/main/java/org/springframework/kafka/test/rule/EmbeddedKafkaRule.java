/*
 * Copyright 2015-2019 the original author or authors.
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

import java.util.Map;

import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

import org.springframework.kafka.test.EmbeddedKafkaBroker;

/**
 * A {@link TestRule} wrapper around an {@link EmbeddedKafkaBroker}.
 *
 * @author Artem Bilan
 *
 * @since 2.2
 *
 * @see EmbeddedKafkaBroker
 */
public class EmbeddedKafkaRule extends ExternalResource {

	private final EmbeddedKafkaBroker embeddedKafka;

	public EmbeddedKafkaRule(int count) {
		this(count, false);
	}

	/**
	 * Create embedded Kafka brokers.
	 * @param count the number of brokers.
	 * @param controlledShutdown passed into TestUtils.createBrokerConfig.
	 * @param topics the topics to create (2 partitions per).
	 */
	public EmbeddedKafkaRule(int count, boolean controlledShutdown, String... topics) {
		this(count, controlledShutdown, 2, topics);
	}

	/**
	 * Create embedded Kafka brokers listening on random ports.
	 * @param count the number of brokers.
	 * @param controlledShutdown passed into TestUtils.createBrokerConfig.
	 * @param partitions partitions per topic.
	 * @param topics the topics to create.
	 */
	public EmbeddedKafkaRule(int count, boolean controlledShutdown, int partitions, String... topics) {
		this.embeddedKafka = new EmbeddedKafkaBroker(count, controlledShutdown, partitions, topics);
	}

	/**
	 * Specify the properties to configure Kafka Broker before start, e.g.
	 * {@code auto.create.topics.enable}, {@code transaction.state.log.replication.factor} etc.
	 * @param brokerProperties the properties to use for configuring Kafka Broker(s).
	 * @return this for chaining configuration
	 * @see kafka.server.KafkaConfig
	 */
	public EmbeddedKafkaRule brokerProperties(Map<String, String> brokerProperties) {
		this.embeddedKafka.brokerProperties(brokerProperties);
		return this;
	}

	/**
	 * Specify a broker property.
	 * @param property the property name.
	 * @param value the value.
	 * @return the {@link EmbeddedKafkaRule}.
	 * @since 2.1.4
	 */
	public EmbeddedKafkaRule brokerProperty(String property, Object value) {
		this.embeddedKafka.brokerProperty(property, value);
		return this;
	}

	/**
	 * Set explicit ports on which the kafka brokers will listen. Useful when running an
	 * embedded broker that you want to access from other processes.
	 * @param kafkaPorts the ports.
	 * @return the rule.
	 */
	public EmbeddedKafkaRule kafkaPorts(int... kafkaPorts) {
		this.embeddedKafka.kafkaPorts(kafkaPorts);
		return this;
	}

	public EmbeddedKafkaRule zkPort(int port) {
		this.embeddedKafka.setZkPort(port);
		return this;
	}

	/**
	 * Return an underlying delegator {@link EmbeddedKafkaBroker} instance.
	 * @return the {@link EmbeddedKafkaBroker} instance.
	 */
	public EmbeddedKafkaBroker getEmbeddedKafka() {
		return this.embeddedKafka;
	}

	@Override
	public void before() {
		this.embeddedKafka.afterPropertiesSet();
	}

	@Override
	public void after() {
		this.embeddedKafka.destroy();
	}

}
