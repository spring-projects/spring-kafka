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

import org.springframework.core.log.LogAccessor;
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

	private final LogAccessor logger;

	private final Level level;

	public LogIfLevelEnabled(LogAccessor logger, Level level) {
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

	public void log(Supplier<CharSequence> messageSupplier) {
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

	public void log(Supplier<CharSequence> messageSupplier, Throwable thrown) {
		switch (this.level) {
			case FATAL:
			fatal(messageSupplier, thrown);
				break;
			case ERROR:
			error(messageSupplier, thrown);
				break;
			case WARN:
			warn(messageSupplier, thrown);
				break;
			case INFO:
			info(messageSupplier, thrown);
				break;
			case DEBUG:
			debug(messageSupplier, thrown);
				break;
			case TRACE:
			trace(messageSupplier, thrown);
				break;
		}
	}

	private void fatal(Supplier<CharSequence> messageSupplier, Throwable thrown) {
		if (thrown != null) {
			this.logger.fatal(thrown, messageSupplier);
		}
		else {
			this.logger.fatal(messageSupplier);
		}
	}

	private void error(Supplier<CharSequence> messageSupplier, Throwable thrown) {
		if (thrown != null) {
			this.logger.error(thrown, messageSupplier);
		}
		else {
			this.logger.error(messageSupplier);
		}
	}

	private void warn(Supplier<CharSequence> messageSupplier, Throwable thrown) {
		if (thrown != null) {
			this.logger.warn(thrown, messageSupplier);
		}
		else {
			this.logger.warn(messageSupplier);
		}
	}

	private void info(Supplier<CharSequence> messageSupplier, Throwable thrown) {
		if (thrown != null) {
			this.logger.info(thrown, messageSupplier);
		}
		else {
			this.logger.info(messageSupplier);
		}
	}

	private void debug(Supplier<CharSequence> messageSupplier, Throwable thrown) {
		if (thrown != null) {
			this.logger.debug(thrown, messageSupplier);
		}
		else {
			this.logger.debug(messageSupplier);
		}
	}

	private void trace(Supplier<CharSequence> messageSupplier, Throwable thrown) {
		if (thrown != null) {
			this.logger.trace(thrown, messageSupplier);
		}
		else {
			this.logger.trace(messageSupplier);
		}
	}

}
