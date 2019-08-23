/*
 * Copyright 2002-2019 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import org.springframework.kafka.test.utils.JUnitUtils;
import org.springframework.kafka.test.utils.JUnitUtils.LevelsContainer;

/**
 * A JUnit method &#064;Rule that changes the logger level for a set of classes
 * while a test method is running. Useful for performance or scalability tests
 * where we don't want to generate a large log in a tight inner loop.
 *
 * @author Dave Syer
 * @author Artem Bilan
 * @author Gary Russell
 *
 */
public class Log4j2LevelAdjuster implements MethodRule {

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
	public Statement apply(final Statement base, FrameworkMethod method, Object target) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				LevelsContainer container = null;
				try {
					container = JUnitUtils.adjustLogLevels(method.getName(),
							Log4j2LevelAdjuster.this.classes, Log4j2LevelAdjuster.this.categories,
							Log4j2LevelAdjuster.this.level);
					base.evaluate();
				}
				finally {
					if (container != null) {
						JUnitUtils.revertLevels(method.getName(), container);
					}
				}
			}

		};
	}

}
