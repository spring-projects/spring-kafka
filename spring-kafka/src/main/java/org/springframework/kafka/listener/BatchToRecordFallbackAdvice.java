/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.kafka.listener;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

import org.springframework.aop.ProxyMethodInvocation;
import org.springframework.kafka.listener.adapter.AsyncRepliesAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Advice that invokes a batch listener with singleton lists after a batch fails.
 * The first singleton failure is reported as a {@link BatchListenerFailedException},
 * allowing the container's error handler to recover that record and redeliver the
 * remainder. An existing {@code BatchListenerFailedException} is propagated unchanged.
 *
 * <p>Add this advice to {@link ContainerProperties#setAdviceChain} before any advice
 * that should also apply to each singleton invocation. Listeners must process records
 * synchronously and be idempotent: records processed before the original batch failure
 * can be processed again. Successful singleton processing must be complete before the
 * listener returns. Account for the additional processing time when configuring
 * {@code max.poll.interval.ms} and {@code max.poll.records}.
 *
 * <p>Fallback is not applied to record listeners, {@code ConsumerRecords} listeners,
 * asynchronous listener adapters, invocations with a manual {@link Acknowledgment},
 * or invocations with an active Spring transaction or thread-bound Spring resources.
 * Such invocations retain their
 * normal behavior. In particular, this advice cannot recover within a failed transaction
 * or safely reuse a whole-batch acknowledgment for individual records.
 *
 * @author Goutam Adwant
 *
 * @since 4.2
 *
 * @see DefaultErrorHandler
 */
public class BatchToRecordFallbackAdvice implements MethodInterceptor {

	@Override
	public @Nullable Object invoke(MethodInvocation invocation) throws Throwable {
		@Nullable Object[] arguments = invocation.getArguments();
		if (!(invocation instanceof ProxyMethodInvocation proxyInvocation)
				|| !(invocation.getThis() instanceof BatchMessageListener)
				|| !"onMessage".equals(invocation.getMethod().getName())
				|| arguments.length == 0 || !(arguments[0] instanceof List<?> records)
				|| records.isEmpty() || isTransactionActive() || Thread.currentThread().isInterrupted()
				|| invocation.getThis() instanceof AsyncRepliesAware async && async.isAsyncReplies()) {

			return invocation.proceed();
		}
		for (@Nullable Object argument : arguments) {
			if (argument instanceof Acknowledgment) {
				return invocation.proceed();
			}
		}
		try {
			return proxyInvocation.invocableClone().proceed();
		}
		catch (Exception ex) {
			if (records.isEmpty() || isTransactionActive()
					|| hasCause(ex, BatchListenerFailedException.class) || isInterruptedOrError(ex)) {
				throw ex;
			}
			if (records.size() == 1) {
				throw new BatchListenerFailedException("Failed to process record", ex,
						(ConsumerRecord<?, ?>) records.get(0));
			}
			for (Object record : records) {
				@Nullable Object[] singletonArguments = arguments.clone();
				singletonArguments[0] = new ArrayList<>(List.of(record));
				try {
					proxyInvocation.invocableClone(singletonArguments).proceed();
				}
				catch (Exception singletonException) {
					if (isInterruptedOrError(singletonException)) {
						throw singletonException;
					}
					throw new BatchListenerFailedException("Failed to process record", singletonException,
							(ConsumerRecord<?, ?>) record);
				}
			}
			return null;
		}
	}

	private boolean isTransactionActive() {
		// A transaction manager can disable synchronization while still binding resources.
		return TransactionSynchronizationManager.isActualTransactionActive()
				|| !TransactionSynchronizationManager.getResourceMap().isEmpty();
	}

	private boolean isInterruptedOrError(Exception ex) {
		return Thread.currentThread().isInterrupted() || hasCause(ex, InterruptedException.class)
				|| hasCause(ex, Error.class);
	}

	private boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
		Set<Throwable> checked = new HashSet<>();
		Throwable current = throwable;
		while (checked.add(current)) {
			if (type.isInstance(current)) {
				return true;
			}
			Throwable cause = current.getCause();
			if (cause == null) {
				return false;
			}
			current = cause;
		}
		return false;
	}

}
