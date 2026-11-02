/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.support;

import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import org.springframework.core.log.LogAccessor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.withSettings;

/**
 * @author Gary Russell
 * @since 2.2.1
 *
 */
@SuppressWarnings("unchecked")
public class LogIfLevelEnabledTests {

	private static final RuntimeException rte = new RuntimeException();

	@Test
	public void testFatalNoEx() {
		LogAccessor theLogger = mock(LogAccessor.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.FATAL);
		logger.log(() -> "foo");
		verify(theLogger).fatal(any(Supplier.class));
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testErrorNoEx() {
		LogAccessor theLogger = mock(LogAccessor.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.ERROR);
		logger.log(() -> "foo");
		verify(theLogger).error(any(Supplier.class));
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testWarnNoEx() {
		LogAccessor theLogger = mock(LogAccessor.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.WARN);
		logger.log(() -> "foo");
		verify(theLogger).warn(any(Supplier.class));
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testInfoNoEx() {
		LogAccessor theLogger = mock(LogAccessor.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.INFO);
		logger.log(() -> "foo");
		verify(theLogger).info(any(Supplier.class));
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testDebugNoEx() {
		LogAccessor theLogger = mock(LogAccessor.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.DEBUG);
		logger.log(() -> "foo");
		verify(theLogger).debug(any(Supplier.class));
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testTraceNoEx() {
		LogAccessor theLogger = mock(LogAccessor.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.TRACE);
		logger.log(() -> "foo");
		verify(theLogger).trace(any(Supplier.class));
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testFatalWithEx() {
		LogAccessor theLogger = mock(LogAccessor.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.FATAL);
		logger.log(() -> "foo", rte);
		verify(theLogger).fatal(any(), any(Supplier.class));
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testErrorWithEx() {
		LogAccessor theLogger = mock(LogAccessor.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.ERROR);
		logger.log(() -> "foo", rte);
		verify(theLogger).error(any(), any(Supplier.class));
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testWarnWithEx() {
		LogAccessor theLogger = mock(LogAccessor.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.WARN);
		logger.log(() -> "foo", rte);
		verify(theLogger).warn(any(), any(Supplier.class));
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testInfoWithEx() {
		LogAccessor theLogger = mock(LogAccessor.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.INFO);
		logger.log(() -> "foo", rte);
		verify(theLogger).info(any(), any(Supplier.class));
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testDebugWithEx() {
		LogAccessor theLogger = mock(LogAccessor.class, withSettings().verboseLogging());
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.DEBUG);
		logger.log(() -> "foo", rte);
		verify(theLogger).debug(any(), any(Supplier.class));
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testTraceWithEx() {
		LogAccessor theLogger = mock(LogAccessor.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.TRACE);
		logger.log(() -> "foo", rte);
		verify(theLogger).trace(any(), any(Supplier.class));
		verifyNoMoreInteractions(theLogger);
	}

}
