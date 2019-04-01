/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.kafka.support;

import java.util.function.Supplier;

import org.springframework.kafka.support.EnhancedLogFactory.Log;
import org.springframework.util.Assert;

/**
 * Wrapper for a commons-logging Log supporting configurable
 * logging levels.
 *
 * @author Gary Russell
 * @since 2.1.2
 *
 */
public final class LogIfLevelEnabled {

	private final Log logger;

	private final Level level;

	public LogIfLevelEnabled(Log logger, Level level) {
		Assert.notNull(logger, "'logger' cannot be null");
		Assert.notNull(level, "'level' cannot be null");
		this.logger = logger;
		this.level = level;
	}

	/**
	 * Logging levels.
	 */
	public enum Level {

		/**
		 * Fatal.
		 */
		FATAL,

		/**
		 * Error.
		 */
		ERROR,

		/**
		 * Warn.
		 */
		WARN,

		/**
		 * Info.
		 */
		INFO,

		/**
		 * Debug.
		 */
		DEBUG,

		/**
		 * Trace.
		 */
		TRACE

	}

	public void log(Supplier<Object> messageSupplier) {
		switch (this.level) {
			case FATAL:
				fatal(messageSupplier, null);
				break;
			case ERROR:
				error(messageSupplier, null);
				break;
			case WARN:
				warn(messageSupplier, null);
				break;
			case INFO:
				info(messageSupplier, null);
				break;
			case DEBUG:
				debug(messageSupplier, null);
				break;
			case TRACE:
				trace(messageSupplier, null);
				break;
		}
	}

	public void log(Supplier<Object> messageSupplier, Throwable t) {
		switch (this.level) {
			case FATAL:
			fatal(messageSupplier, t);
				break;
			case ERROR:
			error(messageSupplier, t);
				break;
			case WARN:
			warn(messageSupplier, t);
				break;
			case INFO:
			info(messageSupplier, t);
				break;
			case DEBUG:
			debug(messageSupplier, t);
				break;
			case TRACE:
			trace(messageSupplier, t);
				break;
		}
	}

	private void fatal(Supplier<Object> messageSupplier, Throwable t) {
		if (t != null) {
			this.logger.fatal(messageSupplier, t);
		}
		else {
			this.logger.fatal(messageSupplier);
		}
	}

	private void error(Supplier<Object> messageSupplier, Throwable t) {
		if (t != null) {
			this.logger.error(messageSupplier, t);
		}
		else {
			this.logger.error(messageSupplier);
		}
	}

	private void warn(Supplier<Object> messageSupplier, Throwable t) {
		if (t != null) {
			this.logger.warn(messageSupplier, t);
		}
		else {
			this.logger.warn(messageSupplier);
		}
	}

	private void info(Supplier<Object> messageSupplier, Throwable t) {
		if (t != null) {
			this.logger.info(messageSupplier, t);
		}
		else {
			this.logger.info(messageSupplier);
		}
	}

	private void debug(Supplier<Object> messageSupplier, Throwable t) {
		if (t != null) {
			this.logger.debug(messageSupplier, t);
		}
		else {
			this.logger.debug(messageSupplier);
		}
	}

	private void trace(Supplier<Object> messageSupplier, Throwable t) {
		if (t != null) {
			this.logger.trace(messageSupplier, t);
		}
		else {
			this.logger.trace(messageSupplier);
		}
	}

}
