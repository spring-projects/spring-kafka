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

package org.springframework.kafka.support;

import org.apache.commons.logging.Log;

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

	public void log(Object message) {
		switch (this.level) {
			case FATAL:
				if (this.logger.isFatalEnabled()) {
					this.logger.fatal(message);
				}
				break;
			case ERROR:
				if (this.logger.isErrorEnabled()) {
					this.logger.error(message);
				}
				break;
			case WARN:
				if (this.logger.isWarnEnabled()) {
					this.logger.warn(message);
				}
				break;
			case INFO:
				if (this.logger.isInfoEnabled()) {
					this.logger.info(message);
				}
				break;
			case DEBUG:
				if (this.logger.isDebugEnabled()) {
					this.logger.debug(message);
				}
				break;
			case TRACE:
				if (this.logger.isTraceEnabled()) {
					this.logger.trace(message);
				}
				break;
		}
	}

	public void log(Object message, Throwable t) {
		switch (this.level) {
			case FATAL:
				if (this.logger.isFatalEnabled()) {
					this.logger.fatal(message, t);
				}
				break;
			case ERROR:
				if (this.logger.isErrorEnabled()) {
					this.logger.error(message, t);
				}
				break;
			case WARN:
				if (this.logger.isWarnEnabled()) {
					this.logger.warn(message, t);
				}
				break;
			case INFO:
				if (this.logger.isInfoEnabled()) {
					this.logger.info(message, t);
				}
				break;
			case DEBUG:
				if (this.logger.isDebugEnabled()) {
					this.logger.debug(message, t);
				}
				break;
			case TRACE:
				if (this.logger.isTraceEnabled()) {
					this.logger.trace(message, t);
				}
				break;
		}
	}

}
