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

package org.springframework.kafka.support;

import java.util.function.Supplier;

import org.apache.commons.logging.LogFactory;

/**
 * Commons logging wrapper with {@link Supplier} log methods.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public final class EnhancedLogFactory {

	private EnhancedLogFactory() {
		super();
	}

	/**
	 * Convenience method to return a named logger.
	 * @param clazz containing Class from which a log name will be derived
	 * @return the log.
	 */
	public static Log getLog(Class<?> clazz) {
		return getLog(clazz.getName());
	}

	/**
	 * Convenience method to return a named logger.
	 * @param name logical name of the <code>Log</code> instance to be returned
	 * @return the log.
	 */
	public static Log getLog(String name) {
		return new Log(LogFactory.getLog(name));
	}

	/**
	 * Wrapper for {@link Log} with {@link Supplier} methods.
	 */
	public static class Log {

		private final org.apache.commons.logging.Log log;

		public Log(org.apache.commons.logging.Log log) {
			this.log = log;
		}

		public boolean isFatalEnabled() {
			return this.log.isFatalEnabled();
		}

		public boolean isErrorEnabled() {
			return this.log.isErrorEnabled();
		}

		public boolean isWarnEnabled() {
			return this.log.isWarnEnabled();
		}

		public boolean isInfoEnabled() {
			return this.log.isInfoEnabled();
		}

		public boolean isDebugEnabled() {
			return this.log.isDebugEnabled();
		}

		public boolean isTraceEnabled() {
			return this.log.isTraceEnabled();
		}

		public void fatal(Object message) {
			this.log.fatal(message);
		}

		public void fatal(Object message, Throwable t) {
			this.log.fatal(message, t);
		}

		public void error(Object message) {
			this.log.error(message);
		}

		public void error(Object message, Throwable t) {
			this.log.error(message, t);
		}

		public void warn(Object message) {
			this.log.warn(message);
		}

		public void warn(Object message, Throwable t) {
			this.log.warn(message, t);
		}

		public void info(Object message) {
			this.log.info(message);
		}

		public void info(Object message, Throwable t) {
			this.log.info(message, t);
		}

		public void debug(Object message) {
			this.log.debug(message);
		}

		public void debug(Object message, Throwable t) {
			this.log.debug(message, t);
		}

		public void trace(Object message) {
			this.log.trace(message);
		}

		public void trace(Object message, Throwable t) {
			this.log.trace(message, t);
		}

		public void fatal(Supplier<Object> supplier) {
			if (this.log.isFatalEnabled()) {
				this.log.fatal(supplier.get());
			}
		}

		public void fatal(Supplier<Object> supplier, Throwable t) {
			if (this.log.isFatalEnabled()) {
				this.log.fatal(supplier.get(), t);
			}
		}

		public void error(Supplier<Object> supplier) {
			if (this.log.isErrorEnabled()) {
				this.log.error(supplier.get());
			}
		}

		public void error(Supplier<Object> supplier, Throwable t) {
			if (this.log.isErrorEnabled()) {
				this.log.error(supplier.get(), t);
			}
		}

		public void warn(Supplier<Object> supplier) {
			if (this.log.isWarnEnabled()) {
				this.log.warn(supplier.get());
			}
		}

		public void warn(Supplier<Object> supplier, Throwable t) {
			if (this.log.isWarnEnabled()) {
				this.log.warn(supplier.get(), t);
			}
		}

		public void info(Supplier<Object> supplier) {
			if (this.log.isInfoEnabled()) {
				this.log.info(supplier.get());
			}
		}

		public void info(Supplier<Object> supplier, Throwable t) {
			if (this.log.isInfoEnabled()) {
				this.log.info(supplier.get(), t);
			}
		}

		public void debug(Supplier<Object> supplier) {
			if (this.log.isDebugEnabled()) {
				this.log.debug(supplier.get());
			}
		}

		public void debug(Supplier<Object> supplier, Throwable t) {
			if (this.log.isDebugEnabled()) {
				this.log.debug(supplier.get(), t);
			}
		}

		public void trace(Supplier<Object> supplier) {
			if (this.log.isTraceEnabled()) {
				this.log.trace(supplier.get());
			}
		}

		public void trace(Supplier<Object> supplier, Throwable t) {
			if (this.log.isTraceEnabled()) {
				this.log.trace(supplier.get(), t);
			}
		}

	}

}
