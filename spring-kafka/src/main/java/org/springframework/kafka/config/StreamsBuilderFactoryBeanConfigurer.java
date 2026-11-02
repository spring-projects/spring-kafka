/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.config;

import org.springframework.core.Ordered;

/**
 * A configurer for {@link StreamsBuilderFactoryBean}. Applied, in order, to the single
 * {@link StreamsBuilderFactoryBean} configured by the framework. Invoked after the bean
 * is created and before it is started. Default order is 0.
 *
 * @author Gary Russell
 * @since 2.6.7
 *
 */
@FunctionalInterface
public interface StreamsBuilderFactoryBeanConfigurer extends Ordered {

	/**
	 * Configure the factory bean.
	 * @param factoryBean the factory bean.
	 */
	void configure(StreamsBuilderFactoryBean factoryBean);

	@Override
	default int getOrder() {
		return 0;
	}

}
