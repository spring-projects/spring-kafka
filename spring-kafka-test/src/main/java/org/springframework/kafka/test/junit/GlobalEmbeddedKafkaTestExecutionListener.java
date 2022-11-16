/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.kafka.test.junit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.util.StringUtils;

/**
 * The {@link TestExecutionListener} to start an {@link EmbeddedKafkaBroker}
 * in the beginning of the test plan and stop in the end.
 * This approach ensures one global Kafka cluster for all the unit tests to execute.
 * <p>
 * The {@link GlobalEmbeddedKafkaTestExecutionListener} is disabled by default.
 * Set {@link GlobalEmbeddedKafkaTestExecutionListener#LISTENER_ENABLED_PROPERTY_NAME}
 * system property (or respective {@link ConfigurationParameters#CONFIG_FILE_NAME} entry)
 * to enable it.
 *
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 3.0
 */
public class GlobalEmbeddedKafkaTestExecutionListener implements TestExecutionListener {

	private static final Log LOGGER = LogFactory.getLog(GlobalEmbeddedKafkaTestExecutionListener.class);

	/**
	 * Property name used to enable the {@code GlobalEmbeddedKafkaTestExecutionListener}.
	 * The {@code GlobalEmbeddedKafkaTestExecutionListener} is registered automatically via
	 * Java's {@link java.util.ServiceLoader} mechanism but disabled by default.
	 * Set the value of this property to {@code true} to enable this listener.
	 */
	public static final String LISTENER_ENABLED_PROPERTY_NAME = "spring.kafka.global.embedded.enabled";

	/**
	 * The number of brokers for {@link EmbeddedKafkaBroker}.
	 */
	public static final String COUNT_PROPERTY_NAME = "spring.kafka.embedded.count";

	/**
	 * The port(s) to expose embedded broker(s).
	 */
	public static final String PORTS_PROPERTY_NAME = "spring.kafka.embedded.ports";

	/**
	 * The topics to create on the embedded broker(s).
	 */
	public static final String TOPICS_PROPERTY_NAME = "spring.kafka.embedded.topics";

	/**
	 * The number of partitions on topics to create on the embedded broker(s).
	 */
	public static final String PARTITIONS_PROPERTY_NAME = "spring.kafka.embedded.partitions";

	/**
	 * The location for a properties file with Kafka broker configuration.
	 */
	public static final String BROKER_PROPERTIES_LOCATION_PROPERTY_NAME =
			"spring.kafka.embedded.broker.properties.location";

	private static final boolean JUNIT_PLATFORM_COMPATIBLE;

	static {
		boolean compat = false;
		try {
			TestPlan.class.getDeclaredMethod("getConfigurationParameters");
			compat = true;
		}
		catch (NoSuchMethodException | SecurityException e) {
			LOGGER.debug("JUnit Platform version must be >= 1.8 to use a global embedded kafka server");
		}
		JUNIT_PLATFORM_COMPATIBLE = compat;
	}

	private EmbeddedKafkaBroker embeddedKafkaBroker;

	@Override
	public void testPlanExecutionStarted(TestPlan testPlan) {
		if (!JUNIT_PLATFORM_COMPATIBLE) {
			return;
		}
		ConfigurationParameters configurationParameters = testPlan.getConfigurationParameters();
		boolean enabled = configurationParameters.getBoolean(LISTENER_ENABLED_PROPERTY_NAME).orElse(false);
		if (enabled) {
			Integer count = configurationParameters.get(COUNT_PROPERTY_NAME, Integer::parseInt).orElse(1);
			String[] topics =
					configurationParameters.get(TOPICS_PROPERTY_NAME, StringUtils::commaDelimitedListToStringArray)
							.orElse(null);
			Integer partitions = configurationParameters.get(PARTITIONS_PROPERTY_NAME, Integer::parseInt).orElse(2);
			Map<String, String> brokerProperties =
					configurationParameters.get(BROKER_PROPERTIES_LOCATION_PROPERTY_NAME, this::brokerProperties)
							.orElse(Map.of());
			String brokerListProperty = configurationParameters.get(EmbeddedKafkaBroker.BROKER_LIST_PROPERTY)
					.orElse(null);
			int[] ports =
					configurationParameters.get(PORTS_PROPERTY_NAME, this::ports)
							.orElse(new int[count]);

			this.embeddedKafkaBroker =
					new EmbeddedKafkaBroker(count, false, partitions, topics)
							.brokerProperties(brokerProperties)
							.brokerListProperty(brokerListProperty)
							.kafkaPorts(ports);
			this.embeddedKafkaBroker.afterPropertiesSet();

			LOGGER.info("Started global Embedded Kafka on: " + this.embeddedKafkaBroker.getBrokersAsString());
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<String, String> brokerProperties(String propertiesLocation) {
		Resource propertiesResource = new DefaultResourceLoader().getResource(propertiesLocation);
		try {
			return (Map) PropertiesLoaderUtils.loadProperties(propertiesResource);
		}
		catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

	private int[] ports(String ports) {
		return StringUtils.commaDelimitedListToSet(ports)
				.stream()
				.mapToInt(Integer::parseInt)
				.toArray();
	}

	@Override
	public void testPlanExecutionFinished(TestPlan testPlan) {
		if (this.embeddedKafkaBroker != null) {
			this.embeddedKafkaBroker.destroy();
			LOGGER.info("Stopped global Embedded Kafka.");
		}
	}

}
