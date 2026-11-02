/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.TaskScheduler;

/**
 * A wrapper class for a {@link TaskScheduler} to use for scheduling container resumption
 * when a partition has been paused for a retry topic. Using this class prevents breaking
 * Spring Boot's auto configuration for other frameworks. Use this if you are using Spring
 * Boot and do not want to use that auto configured scheduler (if it is configured). This
 * framework requires a scheduler bean and looks for one in this order: 1. A single
 * instance of this class, 2. a single {@link TaskScheduler} bean, 3. when multiple
 * {@link TaskScheduler}s are present, a bean with the name {@code taskScheduler}.
 * If you use this class, you should provide a {@link TaskScheduler} that is not defined
 * as a bean; this class will maintain the scheduler's lifecycle.
 *
 * @author Gary Russell
 * @since 2.9
 */
public class RetryTopicSchedulerWrapper implements InitializingBean, DisposableBean {

	private final TaskScheduler scheduler;

	/**
	 * Create a wrapper for the supplied scheduler.
	 * @param scheduler the scheduler
	 */
	public RetryTopicSchedulerWrapper(TaskScheduler scheduler) {
		this.scheduler = scheduler;
	}

	public TaskScheduler getScheduler() {
		return this.scheduler;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (this.scheduler instanceof InitializingBean) {
			((InitializingBean) this.scheduler).afterPropertiesSet();
		}
	}

	@Override
	public void destroy() throws Exception {
		if (this.scheduler instanceof DisposableBean) {
			((DisposableBean) this.scheduler).destroy();
		}

	}

}
