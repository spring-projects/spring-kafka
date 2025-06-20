/*
 * Copyright 2019-present the original author or authors.
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

import java.lang.reflect.AnnotatedElement;
import java.util.Arrays;
import java.util.Optional;

import org.jspecify.annotations.Nullable;
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
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaBrokerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.Assert;

/**
 * JUnit5 condition for an embedded broker.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Pawel Lozinski
 * @author Adrian Chlebosz
 * @author Michał Padula
 *
 * @since 2.3
 *
 */
public class EmbeddedKafkaCondition implements ExecutionCondition, AfterAllCallback, ParameterResolver {

	private static final String EMBEDDED_BROKER = "embedded-kafka";

	private static final ThreadLocal<@Nullable EmbeddedKafkaBroker> BROKERS = new ThreadLocal<>();

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		if (BROKERS.get() == null) {
			return false;
		}
		else {
			return EmbeddedKafkaBroker.class.isAssignableFrom(parameterContext.getParameter().getType());
		}
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
				.map(ExtendWith::value)
				.flatMap(Arrays::stream)
				.anyMatch(SpringExtension.class::isAssignableFrom);
	}

	private EmbeddedKafkaBroker createBroker(EmbeddedKafka embedded) {
		return EmbeddedKafkaBrokerFactory.create(embedded);
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation.
	private @Nullable EmbeddedKafkaBroker getBrokerFromStore(ExtensionContext context) {
		Store parentStore = getParentStore(context);
		EmbeddedKafkaBroker embeddedKafkaBrokerFromParentStore = parentStore == null ? null :
				parentStore
						.get(EMBEDDED_BROKER, EmbeddedKafkaBroker.class);
		return embeddedKafkaBrokerFromParentStore == null
				? getStore(context).get(EMBEDDED_BROKER, EmbeddedKafkaBroker.class)
				: embeddedKafkaBrokerFromParentStore;
	}

	private Store getStore(ExtensionContext context) {
		return context.getStore(Namespace.create(getClass(), context));
	}

	private @Nullable Store getParentStore(ExtensionContext context) {
		ExtensionContext parent = context.getParent().orElse(null);
		return parent == null ? null : parent.getStore(Namespace.create(getClass(), parent));
	}

	public static @Nullable EmbeddedKafkaBroker getBroker() {
		return BROKERS.get();
	}

}
