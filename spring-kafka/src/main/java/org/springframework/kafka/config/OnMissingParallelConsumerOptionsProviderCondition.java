package org.springframework.kafka.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.kafka.core.parallelconsumer.ParallelConsumerOptionsProvider;

public class OnMissingParallelConsumerOptionsProviderCondition implements Condition {

	@Override
	public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
		return context.getBeanFactory().getBean(ParallelConsumerOptionsProvider.class) == null;
	}
}
