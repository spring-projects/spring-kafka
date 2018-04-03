/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.test.rule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * A JUnit method {@link org.junit.Rule} that changes the Log4J 2 logger level for a set of classes
 * or packages while a test method is running. Useful for performance or scalability tests
 * where we don't want to generate a large log in a tight inner loop, or
 * enabling debug logging for a test case.
 *
 * @author Artem Bilan
 *
 * @since 2.2
 *
 */
public final class Log4j2LevelAdjuster implements MethodRule {

	private static final Log logger = LogFactory.getLog(Log4j2LevelAdjuster.class);

	private final Class<?>[] classes;

	private final Level level;

	private final String[] categories;

	private Log4j2LevelAdjuster(Level level) {
		this(level, null, new String[] { "org.springframework.integration" });
	}

	private Log4j2LevelAdjuster(Level level, Class<?>[] classes, String[] categories) {
		Assert.notNull(level, "'level' must be null");
		this.level = level;
		this.classes = classes != null ? classes : new Class<?>[0];

		Stream<String> categoryStream = Stream.of(getClass().getPackage().getName());

		if (!ObjectUtils.isEmpty(categories)) {
			categoryStream = Stream.concat(Arrays.stream(categories), categoryStream);
		}

		this.categories = categoryStream.toArray(String[]::new);
	}

	@Override
	public Statement apply(final Statement base, final FrameworkMethod method, Object target) {
		return new Statement() {

			@Override
			public void evaluate() throws Throwable {
				LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
				Configuration config = ctx.getConfiguration();

				Map<Class<?>, Level> classLevels = new HashMap<>();
				for (Class<?> cls : Log4j2LevelAdjuster.this.classes) {
					String className = cls.getName();
					LoggerConfig loggerConfig = config.getLoggerConfig(className);
					LoggerConfig specificConfig = loggerConfig;

					// We need a specific configuration for this logger,
					// otherwise we would change the level of all other loggers
					// having the original configuration as parent as well

					if (!loggerConfig.getName().equals(className)) {
						specificConfig = new LoggerConfig(className, Log4j2LevelAdjuster.this.level, true);
						specificConfig.setParent(loggerConfig);
						config.addLogger(className, specificConfig);
					}

					classLevels.put(cls, specificConfig.getLevel());
					specificConfig.setLevel(Log4j2LevelAdjuster.this.level);
				}

				Map<String, Level> categoryLevels = new HashMap<>();
				for (String category : Log4j2LevelAdjuster.this.categories) {
					LoggerConfig loggerConfig = config.getLoggerConfig(category);
					LoggerConfig specificConfig = loggerConfig;

					// We need a specific configuration for this logger,
					// otherwise we would change the level of all other loggers
					// having the original configuration as parent as well

					if (!loggerConfig.getName().equals(category)) {
						specificConfig = new LoggerConfig(category, Log4j2LevelAdjuster.this.level, true);
						specificConfig.setParent(loggerConfig);
						config.addLogger(category, specificConfig);
					}

					categoryLevels.put(category, specificConfig.getLevel());
					specificConfig.setLevel(Log4j2LevelAdjuster.this.level);
				}

				ctx.updateLoggers();

				logger.debug("++++++++++++++++++++++++++++ "
						+ "Overridden log level setting for: " + Arrays.toString(Log4j2LevelAdjuster.this.classes)
						+ " and " + Arrays.toString(Log4j2LevelAdjuster.this.categories)
						+ " for test " + method.getName());

				try {
					base.evaluate();
				}
				finally {
					logger.debug("++++++++++++++++++++++++++++ "
							+ "Restoring log level setting for: " + Arrays.toString(Log4j2LevelAdjuster.this.classes)
							+ " and " + Arrays.toString(Log4j2LevelAdjuster.this.categories)
							+ " for test " + method.getName());

					for (Class<?> cls : Log4j2LevelAdjuster.this.classes) {
						LoggerConfig loggerConfig = config.getLoggerConfig(cls.getName());
						loggerConfig.setLevel(classLevels.get(cls));
					}

					for (String category : Log4j2LevelAdjuster.this.categories) {
						LoggerConfig loggerConfig = config.getLoggerConfig(category);
						loggerConfig.setLevel(categoryLevels.get(category));
					}

					ctx.updateLoggers();
				}
			}

		};
	}

	/**
	 * Specify the classes for logging level adjusting configured before.
	 * A new copy Log4j2LevelAdjuster instance is produced by this method.
	 * The provided classes parameter overrides existing value in the {@link #classes}.
	 * @param classes the classes to use for logging level adjusting
	 * @return a Log4j2LevelAdjuster copy with the provided classes
	 */
	public Log4j2LevelAdjuster classes(Class<?>... classes) {
		return classes(false, classes);
	}

	/**
	 * Specify the classes for logging level adjusting configured before.
	 * A new copy Log4j2LevelAdjuster instance is produced by this method.
	 * The provided classes parameter can be merged with existing value in the {@link #classes}.
	 * @param merge to merge or not with previously configured {@link #classes}
	 * @param classes the classes to use for logging level adjusting
	 * @return a Log4j2LevelAdjuster copy with the provided classes
	 * @since 5.0.2
	 */
	public Log4j2LevelAdjuster classes(boolean merge, Class<?>... classes) {
		return new Log4j2LevelAdjuster(this.level,
				merge ? Stream.of(this.classes, classes).flatMap(Stream::of).toArray(Class<?>[]::new) : classes,
				this.categories);
	}

	/**
	 * Specify the categories for logging level adjusting configured before.
	 * A new copy Log4j2LevelAdjuster instance is produced by this method.
	 * The provided categories parameter overrides existing value in the {@link #categories}.
	 * @param categories the categories to use for logging level adjusting
	 * @return a Log4j2LevelAdjuster copy with the provided categories
	 */
	public Log4j2LevelAdjuster categories(String... categories) {
		return categories(false, categories);
	}

	/**
	 * Specify the categories for logging level adjusting configured before.
	 * A new copy Log4j2LevelAdjuster instance is produced by this method.
	 * The provided categories parameter can be merged with existing value in the {@link #categories}.
	 * @param merge to merge or not with previously configured {@link #categories}
	 * @param categories the categories to use for logging level adjusting
	 * @return a Log4j2LevelAdjuster copy with the provided categories
	 * @since 5.0.2
	 */
	public Log4j2LevelAdjuster categories(boolean merge, String... categories) {
		return new Log4j2LevelAdjuster(this.level, this.classes,
				merge ? Stream.of(this.categories, categories).flatMap(Stream::of).toArray(String[]::new) : categories);
	}

	/**
	 * The factory to produce Log4j2LevelAdjuster instances for {@link Level#TRACE} logging
	 * with the {@code org.springframework.integration} as default category.
	 * @return the Log4j2LevelAdjuster instance
	 */
	public static Log4j2LevelAdjuster trace() {
		return forLevel(Level.TRACE);
	}

	/**
	 * The factory to produce Log4j2LevelAdjuster instances for {@link Level#DEBUG} logging
	 * with the {@code org.springframework.integration} as default category.
	 * @return the Log4j2LevelAdjuster instance
	 */
	public static Log4j2LevelAdjuster debug() {
		return forLevel(Level.DEBUG);
	}

	/**
	 * The factory to produce Log4j2LevelAdjuster instances for {@link Level#INFO} logging
	 * with the {@code org.springframework.integration} as default category.
	 * @return the Log4j2LevelAdjuster instance
	 */
	public static Log4j2LevelAdjuster info() {
		return forLevel(Level.INFO);
	}

	/**
	 * The factory to produce Log4j2LevelAdjuster instances for arbitrary logging {@link Level}
	 * with the {@code org.springframework.integration} as default category.
	 * @param level the {@link Level} to use for logging
	 * @return the Log4j2LevelAdjuster instance
	 */
	public static Log4j2LevelAdjuster forLevel(Level level) {
		return new Log4j2LevelAdjuster(level);
	}

}
