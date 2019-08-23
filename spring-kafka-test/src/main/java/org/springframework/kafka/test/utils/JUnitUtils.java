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

package org.springframework.kafka.test.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * Utility methods for JUnit rules and conditions.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.2
 *
 */
public final class JUnitUtils {

	private static final Log LOGGER = LogFactory.getLog(JUnitUtils.class);

	private JUnitUtils() {
		super();
	}

	/**
	 * Return the parsed value if the provided property exists in the environment or system properties.
	 * @param property the property name.
	 * @return the parsed property value if it exists, false otherwise.
	 */
	public static boolean parseBooleanProperty(String property) {
		for (String value : new String[] { System.getenv(property), System.getProperty(property) }) {
			if (Boolean.parseBoolean(value)) {
				return true;
			}
		}
		return false;
	}

	public static LevelsContainer adjustLogLevels(String methodName, List<Class<?>> classes, List<String> categories,
			Level level) {

		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration config = ctx.getConfiguration();

		Map<Class<?>, Level> classLevels = new HashMap<>();
		for (Class<?> cls : classes) {
			String className = cls.getName();
			LoggerConfig loggerConfig = config.getLoggerConfig(className);
			LoggerConfig specificConfig = loggerConfig;

			// We need a specific configuration for this logger,
			// otherwise we would change the level of all other loggers
			// having the original configuration as parent as well

			if (!loggerConfig.getName().equals(className)) {
				specificConfig = new LoggerConfig(className, loggerConfig.getLevel(), true);
				specificConfig.setParent(loggerConfig);
				config.addLogger(className, specificConfig);
			}

			classLevels.put(cls, specificConfig.getLevel());
			specificConfig.setLevel(level);
		}

		Map<String, Level> categoryLevels = new HashMap<>();
		for (String category : categories) {
			LoggerConfig loggerConfig = config.getLoggerConfig(category);
			LoggerConfig specificConfig = loggerConfig;

			// We need a specific configuration for this logger,
			// otherwise we would change the level of all other loggers
			// having the original configuration as parent as well

			if (!loggerConfig.getName().equals(category)) {
				specificConfig = new LoggerConfig(category, loggerConfig.getLevel(), true);
				specificConfig.setParent(loggerConfig);
				config.addLogger(category, specificConfig);
			}

			categoryLevels.put(category, specificConfig.getLevel());
			specificConfig.setLevel(level);
		}

		ctx.updateLoggers();

		LOGGER.info("++++++++++++++++++++++++++++ "
				+ "Overridden log level setting for: "
				+ classes.stream()
				.map(Class::getSimpleName)
				.collect(Collectors.toList())
				+ " and " + categories.toString()
				+ " for test " + methodName);
		return new LevelsContainer(classLevels, categoryLevels);
	}

	public static void revertLevels(String methodName, LevelsContainer container) {
		LOGGER.info("++++++++++++++++++++++++++++ "
				+ "Restoring log level setting for test " + methodName);
		container.oldCatLevels.forEach((key, value) -> {
			((Logger) LogManager.getLogger(key)).setLevel(value);
		});
		container.oldLevels.forEach((key, value) -> {
			((Logger) LogManager.getLogger(key)).setLevel(value);
		});
	}

	public static class LevelsContainer {

		private final Map<Class<?>, Level> oldLevels;

		private final Map<String, Level> oldCatLevels;

		public LevelsContainer(Map<Class<?>, Level> oldLevels, Map<String, Level> oldCatLevels) {

			this.oldLevels = oldLevels;
			this.oldCatLevels = oldCatLevels;
		}

	}

}
