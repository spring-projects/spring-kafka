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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Test;

/**
 * @author Gary Russell
 * @since 2.2.1
 *
 */
public class LogIfLevelEnabledTests {

	private static final RuntimeException rte = new RuntimeException();

	@Test
	public void testFatalNoEx() {
		Log theLogger = mock(Log.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.FATAL);
		given(theLogger.isFatalEnabled()).willReturn(true);
		logger.log(() -> "foo");
		verify(theLogger).isFatalEnabled();
		verify(theLogger).fatal(any());
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testErrorNoEx() {
		Log theLogger = mock(Log.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.ERROR);
		given(theLogger.isFatalEnabled()).willReturn(true);
		given(theLogger.isErrorEnabled()).willReturn(true);
		logger.log(() -> "foo");
		verify(theLogger).isErrorEnabled();
		verify(theLogger).error(any());
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testWarnNoEx() {
		Log theLogger = mock(Log.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.WARN);
		given(theLogger.isFatalEnabled()).willReturn(true);
		given(theLogger.isErrorEnabled()).willReturn(true);
		given(theLogger.isWarnEnabled()).willReturn(true);
		logger.log(() -> "foo");
		verify(theLogger).isWarnEnabled();
		verify(theLogger).warn(any());
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testInfoNoEx() {
		Log theLogger = mock(Log.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.INFO);
		given(theLogger.isFatalEnabled()).willReturn(true);
		given(theLogger.isErrorEnabled()).willReturn(true);
		given(theLogger.isWarnEnabled()).willReturn(true);
		given(theLogger.isInfoEnabled()).willReturn(true);
		logger.log(() -> "foo");
		verify(theLogger).isInfoEnabled();
		verify(theLogger).info(any());
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testDebugNoEx() {
		Log theLogger = mock(Log.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.DEBUG);
		given(theLogger.isFatalEnabled()).willReturn(true);
		given(theLogger.isErrorEnabled()).willReturn(true);
		given(theLogger.isWarnEnabled()).willReturn(true);
		given(theLogger.isInfoEnabled()).willReturn(true);
		given(theLogger.isDebugEnabled()).willReturn(true);
		logger.log(() -> "foo");
		verify(theLogger).isDebugEnabled();
		verify(theLogger).debug(any());
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testTraceNoEx() {
		Log theLogger = mock(Log.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.TRACE);
		given(theLogger.isFatalEnabled()).willReturn(true);
		given(theLogger.isErrorEnabled()).willReturn(true);
		given(theLogger.isWarnEnabled()).willReturn(true);
		given(theLogger.isInfoEnabled()).willReturn(true);
		given(theLogger.isDebugEnabled()).willReturn(true);
		given(theLogger.isTraceEnabled()).willReturn(true);
		logger.log(() -> "foo");
		verify(theLogger).isTraceEnabled();
		verify(theLogger).trace(any());
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testFatalWithEx() {
		Log theLogger = mock(Log.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.FATAL);
		given(theLogger.isFatalEnabled()).willReturn(true);
		logger.log(() -> "foo", rte);
		verify(theLogger).isFatalEnabled();
		verify(theLogger).fatal(any(), any());
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testErrorWithEx() {
		Log theLogger = mock(Log.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.ERROR);
		given(theLogger.isFatalEnabled()).willReturn(true);
		given(theLogger.isErrorEnabled()).willReturn(true);
		logger.log(() -> "foo", rte);
		verify(theLogger).isErrorEnabled();
		verify(theLogger).error(any(), any());
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testWarnWithEx() {
		Log theLogger = mock(Log.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.WARN);
		given(theLogger.isFatalEnabled()).willReturn(true);
		given(theLogger.isErrorEnabled()).willReturn(true);
		given(theLogger.isWarnEnabled()).willReturn(true);
		logger.log(() -> "foo", rte);
		verify(theLogger).isWarnEnabled();
		verify(theLogger).warn(any(), any());
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testInfoWithEx() {
		Log theLogger = mock(Log.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.INFO);
		given(theLogger.isFatalEnabled()).willReturn(true);
		given(theLogger.isErrorEnabled()).willReturn(true);
		given(theLogger.isWarnEnabled()).willReturn(true);
		given(theLogger.isInfoEnabled()).willReturn(true);
		logger.log(() -> "foo", rte);
		verify(theLogger).isInfoEnabled();
		verify(theLogger).info(any(), any());
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testDebugWithEx() {
		Log theLogger = mock(Log.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.DEBUG);
		given(theLogger.isFatalEnabled()).willReturn(true);
		given(theLogger.isErrorEnabled()).willReturn(true);
		given(theLogger.isWarnEnabled()).willReturn(true);
		given(theLogger.isInfoEnabled()).willReturn(true);
		given(theLogger.isDebugEnabled()).willReturn(true);
		logger.log(() -> "foo", rte);
		verify(theLogger).isDebugEnabled();
		verify(theLogger).debug(any(), any());
		verifyNoMoreInteractions(theLogger);
	}

	@Test
	public void testTraceWithEx() {
		Log theLogger = mock(Log.class);
		LogIfLevelEnabled logger = new LogIfLevelEnabled(theLogger, LogIfLevelEnabled.Level.TRACE);
		given(theLogger.isFatalEnabled()).willReturn(true);
		given(theLogger.isErrorEnabled()).willReturn(true);
		given(theLogger.isWarnEnabled()).willReturn(true);
		given(theLogger.isInfoEnabled()).willReturn(true);
		given(theLogger.isDebugEnabled()).willReturn(true);
		given(theLogger.isTraceEnabled()).willReturn(true);
		logger.log(() -> "foo", rte);
		verify(theLogger).isTraceEnabled();
		verify(theLogger).trace(any(), any());
		verifyNoMoreInteractions(theLogger);
	}

}
