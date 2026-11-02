/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka;

import org.jspecify.annotations.Nullable;

import org.springframework.core.NestedRuntimeException;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

/**
 * The Spring for Apache Kafka specific {@link NestedRuntimeException} implementation.
 *
 * @author Gary Russell
 * @author Artem Bilan
 */
@SuppressWarnings("serial")
public class KafkaException extends NestedRuntimeException {

	/**
	 * The log level for {@link KafkaException}.
	 * @since 2.5
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

	private final Level logLevel;

	/**
	 * Construct an instance with the provided properties.
	 * @param message the message.
	 */
	public KafkaException(String message) {
		this(message, Level.ERROR, null);
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param message the message.
	 * @param cause the cause.
	 */
	public KafkaException(String message, @Nullable Throwable cause) {
		this(message, Level.ERROR, cause);
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param message the message.
	 * @param level the level at which this exception should be logged when using
	 * {@link #selfLog(String, LogAccessor)}.
	 * @param cause the cause.
	 */
	public KafkaException(String message, Level level, @Nullable Throwable cause) {
		super(message, cause);
		Assert.notNull(level, "'level' cannot be null");
		this.logLevel = level;
	}

	/**
	 * Log this exception at its log level.
	 * @param message the message.
	 * @param logger the log accessor.
	 */
	public void selfLog(String message, LogAccessor logger) {
		switch (this.logLevel) {
			case FATAL -> logger.fatal(this, message);
			case ERROR -> logger.error(this, message);
			case WARN -> logger.warn(this, message);
			case INFO -> logger.info(this, message);
			case DEBUG -> logger.debug(this, message);
			case TRACE -> logger.trace(this, message);
			default -> logger.error(this, message);
		}
	}

}
