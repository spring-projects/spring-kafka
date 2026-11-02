/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.kafka.listener.adapter;

import java.lang.reflect.Method;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.core.KotlinDetector;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolverComposite;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.validation.Validator;

/**
 * Extension of the {@link DefaultMessageHandlerMethodFactory} for Spring Kafka requirements.
 *
 * @author Wang Zhiyang
 *
 * @since 3.2
 */
public class KafkaMessageHandlerMethodFactory extends DefaultMessageHandlerMethodFactory {

	private final HandlerMethodArgumentResolverComposite argumentResolvers =
			new HandlerMethodArgumentResolverComposite();

	@SuppressWarnings("NullAway.Init")
	private MessageConverter messageConverter;

	private @Nullable Validator validator;

	@Override
	public void setMessageConverter(MessageConverter messageConverter) {
		super.setMessageConverter(messageConverter);
		this.messageConverter = messageConverter;
	}

	@Override
	public void setValidator(Validator validator) {
		super.setValidator(validator);
		this.validator = validator;
	}

	@Override
	protected List<HandlerMethodArgumentResolver> initArgumentResolvers() {
		List<HandlerMethodArgumentResolver> resolvers = super.initArgumentResolvers();
		if (KotlinDetector.isKotlinPresent()) {
			// Insert before PayloadMethodArgumentResolver
			resolvers.add(resolvers.size() - 1, new ContinuationHandlerMethodArgumentResolver());
		}
		// Has to be at the end - look at PayloadMethodArgumentResolver documentation
		resolvers.add(resolvers.size() - 1, new KafkaNullAwarePayloadArgumentResolver(this.messageConverter, this.validator));
		this.argumentResolvers.addResolvers(resolvers);
		return resolvers;
	}

	@Override
	public InvocableHandlerMethod createInvocableHandlerMethod(Object bean, Method method) {
		InvocableHandlerMethod handlerMethod = new KotlinAwareInvocableHandlerMethod(bean, method);
		handlerMethod.setMessageMethodArgumentResolvers(this.argumentResolvers);
		return handlerMethod;
	}

}
