/*
 * Copyright 2016-2024 the original author or authors.
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

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.kafka.core.parallelconsumer.ParallelConsumerOptionsProvider;

/**
 * This class is to provide {@link ParallelConsumerOptionsProvider} as default.
 * {@link ParallelConsumerConfiguration} use this function when {@link ApplicationContext} started.
 * If there is no spring bean in {@link ApplicationContext} such as {@link ParallelConsumerOptionsProvider},
 * {@link ParallelConsumerConfiguration} will register default {@link ParallelConsumerOptionsProvider}.
 *
 * @author Sanghyeok An
 *
 * @since 3.3
 */
public class OnMissingParallelConsumerOptionsProviderCondition implements Condition {

	@Override
	public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
		return context.getBeanFactory().getBean(ParallelConsumerOptionsProvider.class) == null;
	}
}
