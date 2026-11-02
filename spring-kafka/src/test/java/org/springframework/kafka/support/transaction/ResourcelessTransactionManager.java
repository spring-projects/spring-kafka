/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.support.transaction;

import java.util.ArrayList;
import java.util.List;

import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@SuppressWarnings("serial")
public final class ResourcelessTransactionManager extends AbstractPlatformTransactionManager {

	@Override
	public void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		((ResourcelessTransaction) transaction).begin();
	}

	@Override
	public void doCommit(DefaultTransactionStatus status) throws TransactionException {
		if (logger.isDebugEnabled()) {
			logger.debug("Committing resourceless transaction on [" + status.getTransaction() + "]");
		}
	}

	@Override
	protected Object doGetTransaction() throws TransactionException {
		Object transaction = new ResourcelessTransaction();
		List<Object> resources;
		if (!TransactionSynchronizationManager.hasResource(this)) {
			resources = new ArrayList<>();
			TransactionSynchronizationManager.bindResource(this, resources);
		}
		else {
			@SuppressWarnings("unchecked")
			List<Object> stack = (List<Object>) TransactionSynchronizationManager.getResource(this);
			resources = stack;
		}
		resources.add(transaction);
		return transaction;
	}

	@Override
	protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		if (logger.isDebugEnabled()) {
			logger.debug("Rolling back resourceless transaction on [" + status.getTransaction() + "]");
		}
	}

	@Override
	protected boolean isExistingTransaction(Object transaction) throws TransactionException {
		if (TransactionSynchronizationManager.hasResource(this)) {
			List<?> stack = (List<?>) TransactionSynchronizationManager.getResource(this);
			return stack.size() > 1;
		}
		return ((ResourcelessTransaction) transaction).isActive();
	}

	@Override
	protected void doSetRollbackOnly(DefaultTransactionStatus status) throws TransactionException {
	}

	@Override
	protected void doCleanupAfterCompletion(Object transaction) {
		List<?> resources = (List<?>) TransactionSynchronizationManager.getResource(this);
		resources.clear();
		TransactionSynchronizationManager.unbindResource(this);
		((ResourcelessTransaction) transaction).clear();
	}

	private static final class ResourcelessTransaction {

		private boolean active = false;

		public boolean isActive() {
			return active;
		}

		public void begin() {
			active = true;
		}

		public void clear() {
			active = false;
		}

	}
}
