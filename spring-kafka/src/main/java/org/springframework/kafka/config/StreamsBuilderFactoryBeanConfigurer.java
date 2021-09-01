/*
 * Copyright 2021 the original author or authors.
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
