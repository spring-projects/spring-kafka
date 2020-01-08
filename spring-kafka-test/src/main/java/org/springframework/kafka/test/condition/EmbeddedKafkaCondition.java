/*
 * Copyright 2019-2020 the original author or authors.
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

package org.springframework.kafka.test.condition;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.AnnotatedElement;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * JUnit5 condition for an embedded broker.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.3
 *
 */
public class EmbeddedKafkaCondition implements ExecutionCondition, AfterAllCallback, ParameterResolver {

	private static final String EMBEDDED_BROKER = "embedded-kafka";

	private static final ThreadLocal<EmbeddedKafkaBroker> BROKERS = new ThreadLocal<>();

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		return parameterContext.getParameter().getType().equals(EmbeddedKafkaBroker.class);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context)
			throws ParameterResolutionException {

		EmbeddedKafkaBroker broker = getBrokerFromStore(context);
		Assert.state(broker != null, "Could not find embedded broker instance");
		return broker;
	}

	@Override
	public void afterAll(ExtensionContext context) {
		EmbeddedKafkaBroker broker = BROKERS.get();
		if (broker != null) {
			broker.destroy();
			BROKERS.remove();
		}
	}

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		Optional<AnnotatedElement> element = context.getElement();
		if (element.isPresent() && !springTestContext(element.get())) {

			EmbeddedKafka embedded = AnnotatedElementUtils.findMergedAnnotation(element.get(), EmbeddedKafka.class);
			// When running in a spring test context, the EmbeddedKafkaContextCustomizer will create the broker.
			if (embedded != null) {
				EmbeddedKafkaBroker broker = getBrokerFromStore(context);
				if (broker == null) {
					broker = createBroker(embedded);
					BROKERS.set(broker);
					getStore(context).put(EMBEDDED_BROKER, broker);
				}
			}
		}
		return ConditionEvaluationResult.enabled("");
	}

	private boolean springTestContext(AnnotatedElement annotatedElement) {
		return AnnotatedElementUtils.findAllMergedAnnotations(annotatedElement, ExtendWith.class)
				.stream()
				.filter(extended -> Arrays.asList(extended.value()).contains(SpringExtension.class))
				.findFirst()
				.isPresent();
	}

	@SuppressWarnings("unchecked")
	private EmbeddedKafkaBroker createBroker(EmbeddedKafka embedded) {
		EmbeddedKafkaBroker broker;
		int[] ports = embedded.ports();
		if (embedded.count() > 1 && ports.length == 1 && ports[0] == 0) {
			ports = new int[embedded.count()];
		}
		broker = new EmbeddedKafkaBroker(embedded.count(), embedded.controlledShutdown(), embedded.topics())
				.zkPort(embedded.zookeeperPort())
				.kafkaPorts(ports);
		Properties properties = new Properties();

		for (String pair : embedded.brokerProperties()) {
			if (!StringUtils.hasText(pair)) {
				continue;
			}
			try {
				properties.load(new StringReader(pair));
			}
			catch (Exception ex) {
				throw new IllegalStateException("Failed to load broker property from [" + pair + "]",
						ex);
			}
		}
		if (StringUtils.hasText(embedded.brokerPropertiesLocation())) {
			Resource propertiesResource = new PathMatchingResourcePatternResolver()
					.getResource(embedded.brokerPropertiesLocation());
			if (!propertiesResource.exists()) {
				throw new IllegalStateException(
						"Failed to load broker properties from [" + propertiesResource
								+ "]: resource does not exist.");
			}
			try (InputStream in = propertiesResource.getInputStream()) {
				Properties p = new Properties();
				p.load(in);
				p.forEach(properties::putIfAbsent);
			}
			catch (IOException ex) {
				throw new IllegalStateException(
						"Failed to load broker properties from [" + propertiesResource + "]", ex);
			}
		}
		broker.brokerProperties((Map<String, String>) (Map<?, ?>) properties);
		if (StringUtils.hasText(embedded.bootstrapServersProperty())) {
			broker.brokerListProperty(embedded.bootstrapServersProperty());
		}
		broker.afterPropertiesSet();
		return broker;
	}

	private EmbeddedKafkaBroker getBrokerFromStore(ExtensionContext context) {
		return getParentStore(context).get(EMBEDDED_BROKER, EmbeddedKafkaBroker.class) == null
				? getStore(context).get(EMBEDDED_BROKER, EmbeddedKafkaBroker.class)
				: getParentStore(context).get(EMBEDDED_BROKER, EmbeddedKafkaBroker.class);
	}

	private Store getStore(ExtensionContext context) {
		return context.getStore(Namespace.create(getClass(), context));
	}

	private Store getParentStore(ExtensionContext context) {
		ExtensionContext parent = context.getParent().get();
		return parent.getStore(Namespace.create(getClass(), parent));
	}


	public static EmbeddedKafkaBroker getBroker() {
		return BROKERS.get();
	}

}
