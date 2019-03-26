/*
 * Copyright 2017 the original author or authors.
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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 * A JUnit method &#064;Rule that changes the logger level for a set of classes
 * or packages
 * while a test method is running. Useful for performance or scalability tests
 * where we don't want to generate a large log in a tight inner loop, or
 * enabling debug logging for a test case.
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 * @since 2.0
 */
public class Log4jLevelAdjuster implements MethodRule {

	private static final Log logger = LogFactory.getLog(Log4jLevelAdjuster.class);

	private final Class<?>[] classes;

	private final Level level;

	private final String[] categories;

	public Log4jLevelAdjuster(Level level, Class<?>... classes) {
		this.level = level;
		this.classes = classes;
		this.categories = new String[0];
	}

	public Log4jLevelAdjuster(Level level, String... categories) {
		this.level = level;
		this.classes = new Class<?>[0];
		Set<String> cats = new LinkedHashSet<String>(Arrays.asList(categories));
		cats.add(getClass().getPackage().getName());
		this.categories = new ArrayList<String>(cats).toArray(new String[cats.size()]);
	}

	@Override
	public Statement apply(final Statement base, final FrameworkMethod method, Object target) {
		return new Statement() {

			@Override
			public void evaluate() throws Throwable {
				Map<Class<?>, Level> oldLevels = new HashMap<Class<?>, Level>();
				for (Class<?> cls : classes) {
					oldLevels.put(cls, LogManager.getLogger(cls).getEffectiveLevel());
					LogManager.getLogger(cls).setLevel(level);
				}
				Map<String, Level> oldCatLevels = new HashMap<String, Level>();
				for (String category : categories) {
					oldCatLevels.put(category, LogManager.getLogger(category).getEffectiveLevel());
					LogManager.getLogger(category).setLevel(level);
				}
				logger.debug("++++++++++++++++++++++++++++ "
						+ "Overridden log level setting for: " + Arrays.asList(classes) + " and "
						+ Arrays.asList(categories) + " for test " + method.getName());
				try {
					base.evaluate();
				}
				finally {
					logger.debug("++++++++++++++++++++++++++++ "
							+ "Restoring log level setting for: " + Arrays.asList(classes) + " and "
							+ Arrays.asList(categories) + " for test " + method.getName());
					// raw Class type used to avoid https://bugs.java.com/view_bug.do?bug_id=6682380
					for (Class<?> cls : classes) {
						LogManager.getLogger(cls).setLevel(oldLevels.get(cls));
					}
					for (String category : categories) {
						LogManager.getLogger(category).setLevel(oldCatLevels.get(category));
					}
				}
			}
		};
	}

}
