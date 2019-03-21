/*
 * Copyright 2018 the original author or authors.
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

import org.apache.commons.logging.Log;

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
				if (this.logger.isFatalEnabled()) {
					this.logger.fatal(messageSupplier.get());
				}
				break;
			case ERROR:
				if (this.logger.isErrorEnabled()) {
					this.logger.error(messageSupplier.get());
				}
				break;
			case WARN:
				if (this.logger.isWarnEnabled()) {
					this.logger.warn(messageSupplier.get());
				}
				break;
			case INFO:
				if (this.logger.isInfoEnabled()) {
					this.logger.info(messageSupplier.get());
				}
				break;
			case DEBUG:
				if (this.logger.isDebugEnabled()) {
					this.logger.debug(messageSupplier.get());
				}
				break;
			case TRACE:
				if (this.logger.isTraceEnabled()) {
					this.logger.trace(messageSupplier.get());
				}
				break;
		}
	}

	public void log(Supplier<Object> messageSupplier, Throwable t) {
		switch (this.level) {
			case FATAL:
				if (this.logger.isFatalEnabled()) {
					this.logger.fatal(messageSupplier.get(), t);
				}
				break;
			case ERROR:
				if (this.logger.isErrorEnabled()) {
					this.logger.error(messageSupplier.get(), t);
				}
				break;
			case WARN:
				if (this.logger.isWarnEnabled()) {
					this.logger.warn(messageSupplier.get(), t);
				}
				break;
			case INFO:
				if (this.logger.isInfoEnabled()) {
					this.logger.info(messageSupplier.get(), t);
				}
				break;
			case DEBUG:
				if (this.logger.isDebugEnabled()) {
					this.logger.debug(messageSupplier.get(), t);
				}
				break;
			case TRACE:
				if (this.logger.isTraceEnabled()) {
					this.logger.trace(messageSupplier.get(), t);
				}
				break;
		}
	}

}
