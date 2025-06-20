/*
 * Copyright 2024-present the original author or authors.
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
import java.io.InputStream;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.util.StringUtils;

/**
 * The factory to encapsulate an {@link EmbeddedKafkaBroker} creation logic.
 *
 * @author Artem Bilan
 *
 * @since 3.2.6
 */
public final class EmbeddedKafkaBrokerFactory {

	private static final String TRANSACTION_STATE_LOG_REPLICATION_FACTOR = "transaction.state.log.replication.factor";

	/**
	 * Create an {@link EmbeddedKafkaBroker} based on the {@code EmbeddedKafka} annotation.
	 * @param embeddedKafka the {@code EmbeddedKafka} annotation.
	 * @return a new {@link EmbeddedKafkaBroker} instance.
	 */
	public static EmbeddedKafkaBroker create(EmbeddedKafka embeddedKafka) {
		return create(embeddedKafka, Function.identity());
	}

	/**
	 * Create an {@link EmbeddedKafkaBroker} based on the {@code EmbeddedKafka} annotation.
	 * @param embeddedKafka the {@code EmbeddedKafka} annotation.
	 * @param propertyResolver the {@link Function} for placeholders in the annotation attributes.
	 * @return a new {@link EmbeddedKafkaBroker} instance.
	 */
	@SuppressWarnings("unchecked")
	public static EmbeddedKafkaBroker create(EmbeddedKafka embeddedKafka, Function<String, String> propertyResolver) {
		String[] topics =
				Arrays.stream(embeddedKafka.topics())
						.map(propertyResolver)
						.toArray(String[]::new);

		EmbeddedKafkaBroker embeddedKafkaBroker;
		embeddedKafkaBroker = kraftBroker(embeddedKafka, topics);

		int[] ports = setupPorts(embeddedKafka);

		embeddedKafkaBroker.kafkaPorts(ports)
				.adminTimeout(embeddedKafka.adminTimeout());

		Properties properties = new Properties();

		for (String pair : embeddedKafka.brokerProperties()) {
			if (!StringUtils.hasText(pair)) {
				continue;
			}
			try {
				properties.load(new StringReader(propertyResolver.apply(pair)));
			}
			catch (Exception ex) {
				throw new IllegalStateException("Failed to load broker property from [" + pair + "]", ex);
			}
		}

		String brokerPropertiesLocation = embeddedKafka.brokerPropertiesLocation();
		if (StringUtils.hasText(brokerPropertiesLocation)) {
			String propertiesLocation = propertyResolver.apply(brokerPropertiesLocation);
			Resource propertiesResource = new PathMatchingResourcePatternResolver().getResource(propertiesLocation);
			if (!propertiesResource.exists()) {
				throw new IllegalStateException(
						"Failed to load broker properties from [" + propertiesResource + "]: resource does not exist.");
			}
			try (InputStream in = propertiesResource.getInputStream()) {
				Properties p = new Properties();
				p.load(in);
				p.forEach((key, value) -> properties.putIfAbsent(key, propertyResolver.apply((String) value)));
			}
			catch (IOException ex) {
				throw new IllegalStateException("Failed to load broker properties from [" + propertiesResource + "]", ex);
			}
		}

		properties.putIfAbsent(TRANSACTION_STATE_LOG_REPLICATION_FACTOR,
				String.valueOf(Math.min(3, embeddedKafka.count())));

		embeddedKafkaBroker.brokerProperties((Map<String, String>) (Map<?, ?>) properties);
		String bootstrapServersProperty = embeddedKafka.bootstrapServersProperty();
		if (StringUtils.hasText(bootstrapServersProperty)) {
			embeddedKafkaBroker.brokerListProperty(bootstrapServersProperty);
		}

		// Safe to start an embedded broker eagerly before context refresh
		embeddedKafkaBroker.afterPropertiesSet();

		return embeddedKafkaBroker;
	}

	private static int[] setupPorts(EmbeddedKafka embedded) {
		int[] ports = embedded.ports();
		if (embedded.count() > 1 && ports.length == 1 && ports[0] == 0) {
			ports = new int[embedded.count()];
		}
		return ports;
	}

	private static EmbeddedKafkaBroker kraftBroker(EmbeddedKafka embedded, String[] topics) {
		return new EmbeddedKafkaKraftBroker(embedded.count(), embedded.partitions(), topics);
	}

	private EmbeddedKafkaBrokerFactory() {
	}

}
