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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.withSettings;

import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import org.springframework.core.log.LogAccessor;

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
