/*
 * Copyright 2002-2025 the original author or authors.
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

package org.springframework.kafka.test.extensions;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import org.springframework.kafka.test.utils.JUnitUtils;
import org.springframework.kafka.test.utils.JUnitUtils.LevelsContainer;

/**
 * A JUnit extension &#064; that changes the logger level for a set of classes
 * while a test method is running. Useful for performance or scalability tests
 * where we don't want to generate a large log in a tight inner loop.
 *
 * @author Dave Syer
 * @author Artem Bilan
 * @author Gary Russell
 * @author Sanghyoek An
 *
 */
public class Log4j2LevelAdjuster implements InvocationInterceptor {

	private final List<Class<?>> classes;

	private List<String> categories;

	private final Level level;

	public Log4j2LevelAdjuster(Level level, Class<?>... classes) {
		this.level = level;
		this.classes = new ArrayList<>(Arrays.asList(classes));
		this.classes.add(getClass());
		this.categories = Collections.emptyList();
	}

	public Log4j2LevelAdjuster categories(String... categoriesToAdjust) {
		this.categories = new ArrayList<>(Arrays.asList(categoriesToAdjust));
		return this;
	}

	@Override
	public void interceptTestMethod(Invocation<Void> invocation,
									ReflectiveInvocationContext<Method> invocationContext,
									ExtensionContext extensionContext) throws Throwable {
		String methodName = extensionContext.getRequiredTestMethod().getName();
		LevelsContainer container = null;

		try {
			container = JUnitUtils.adjustLogLevels(
					methodName,
					Log4j2LevelAdjuster.this.classes,
					Log4j2LevelAdjuster.this.categories,
					Log4j2LevelAdjuster.this.level);
			invocation.proceed();
		}
		finally {
			if (container != null) {
				JUnitUtils.revertLevels(methodName, container);
			}
		}
	}

}
