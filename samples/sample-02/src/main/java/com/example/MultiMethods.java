/*
 * Copyright 2018-present the original author or authors.
 */

package com.example;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.common.Bar2;
import com.common.Foo2;

/**
 * @author Gary Russell
 * @since 5.1
 *
 */
@Component
@KafkaListener(id = "multiGroup", topics = { "foos", "bars" })
public class MultiMethods {

	private final TaskExecutor exec = new SimpleAsyncTaskExecutor();

	@KafkaHandler
	public void foo(Foo2 foo) {
		System.out.println("Received: " + foo);
		terminateMessage();
	}

	@KafkaHandler
	public void bar(Bar2 bar) {
		System.out.println("Received: " + bar);
		terminateMessage();
	}

	@KafkaHandler(isDefault = true)
	public void unknown(Object object) {
		System.out.println("Received unknown: " + object);
		terminateMessage();
	}

	private void terminateMessage() {
		this.exec.execute(() -> System.out.println("Hit Enter to terminate..."));
	}

}
