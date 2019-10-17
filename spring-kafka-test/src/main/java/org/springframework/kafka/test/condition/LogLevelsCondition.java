/*
 * Copyright 2019 the original author or authors.
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
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.test.utils.JUnitUtils;
import org.springframework.kafka.test.utils.JUnitUtils.LevelsContainer;

/**
 * JUnit condition that adjusts and reverts log levels before/after each test.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public class LogLevelsCondition
		implements ExecutionCondition, BeforeEachCallback, AfterEachCallback, BeforeAllCallback, AfterAllCallback {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(LogLevelsCondition.class));

	private static final String STORE_ANNOTATION_KEY = "logLevelsAnnotation";

	private static final String STORE_CONTAINER_KEY = "logLevelsContainer";

	private static final ConditionEvaluationResult ENABLED =
			ConditionEvaluationResult.enabled("@LogLevels always enabled");

	private final Map<String, Boolean> loggedMethods = new ConcurrentHashMap<>();

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		Optional<AnnotatedElement> element = context.getElement();
		MergedAnnotations annotations = MergedAnnotations.from(element.get(),
				MergedAnnotations.SearchStrategy.TYPE_HIERARCHY);
		MergedAnnotation<LogLevels> mergedAnnotation = annotations.get(LogLevels.class);
		if (mergedAnnotation.isPresent()) {
			LogLevels loglevels = mergedAnnotation.synthesize();
			Store store = context.getStore(Namespace.create(getClass(), context));
			store.put(STORE_ANNOTATION_KEY, loglevels);
		}
		return ENABLED;
	}

	@Override
	public void beforeAll(ExtensionContext context) {
		Store store = context.getStore(Namespace.create(getClass(), context));
		LogLevels logLevels = store.get(STORE_ANNOTATION_KEY, LogLevels.class);
		if (logLevels != null) {
			store.put(STORE_CONTAINER_KEY, JUnitUtils.adjustLogLevels(context.getDisplayName(),
					Arrays.asList((logLevels.classes())),
					Arrays.asList(logLevels.categories()),
					Level.toLevel(logLevels.level())));
		}
	}

	@Override
	public void beforeEach(ExtensionContext context) {
		Store store = context.getStore(Namespace.create(getClass(), context));
		LogLevels logLevels = store.get(STORE_ANNOTATION_KEY, LogLevels.class);
		if (logLevels != null) { // Method level annotation
			if (store.get(STORE_CONTAINER_KEY) == null) {
				store.put(STORE_CONTAINER_KEY, JUnitUtils.adjustLogLevels(context.getDisplayName(),
						Arrays.asList((logLevels.classes())),
						Arrays.asList(logLevels.categories()),
						Level.toLevel(logLevels.level())));
			}
		}
		else {
			Optional<Method> testMethod = context.getTestMethod();
			if (testMethod.isPresent()
					&& this.loggedMethods.putIfAbsent(testMethod.get().getName(), Boolean.TRUE) == null) {
				LOGGER.info(() -> "+++++++++++++++++++++++++++++ Begin " + testMethod.get().getName());
			}
		}
	}

	@Override
	public void afterEach(ExtensionContext context) {
		Store store = context.getStore(Namespace.create(getClass(), context));
		LevelsContainer container = store.get(STORE_CONTAINER_KEY, LevelsContainer.class);
		if (container != null) {
			LogLevels logLevels = store.get(STORE_ANNOTATION_KEY, LogLevels.class);
			if (logLevels != null) {
				JUnitUtils.revertLevels(context.getDisplayName(), container);
				store.remove(STORE_CONTAINER_KEY);
			}
		}
	}

	@Override
	public void afterAll(ExtensionContext context) {
		Store store = context.getStore(Namespace.create(getClass(), context));
		LogLevels logLevels = store.remove(STORE_ANNOTATION_KEY, LogLevels.class);
		if (logLevels != null) {
			LevelsContainer container = store.get(STORE_CONTAINER_KEY, LevelsContainer.class);
			JUnitUtils.revertLevels(context.getDisplayName(), container);
			store.remove(STORE_CONTAINER_KEY);
		}
		this.loggedMethods.clear();
	}

}
