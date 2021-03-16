/*
 * Copyright 2020-2021 the original author or authors.
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

/**
 * A customizer for the {@link StreamsBuilderFactoryBean} that is implicitly created by
 * {@link org.springframework.kafka.annotation.EnableKafkaStreams}. If exactly one
 * implementation of this interface is found in the application context (or one is marked
 * as {@link org.springframework.context.annotation.Primary}, it will be invoked after the
 * factory bean has been created and before it is started.
 * @deprecated in favor of {@code StreamsBuilderFactoryBeanConfigurer} due to a name
 * clash with a similar class in Spring Boot.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
@Deprecated
@FunctionalInterface
public interface StreamsBuilderFactoryBeanCustomizer {

	/**
	 * Configure the factory bean.
	 * @param factoryBean the factory bean.
	 */
	void configure(StreamsBuilderFactoryBean factoryBean);

}
