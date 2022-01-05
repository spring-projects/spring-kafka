/*
 * Copyright 2019-2021 the original author or authors.
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

package org.springframework.kafka.support.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.annotation.ResolvableType;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.ByteArrayMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.DefaultContentTypeResolver;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.GenericMessage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Scruel Tao
 */
public class ResolvableTypeResolverTests {
	private static final String TEXT = "kafka";

	@Test
	void jsonFoo() throws Exception {
		InvocableHandlerMethod method = getFooInvocableHandlerMethod();
		String s = new ObjectMapper().writeValueAsString(new Foo().setText(TEXT));
		method.invoke(new GenericMessage<>(s));
	}

	@Test
	void bytesToFoo() throws Exception {
		InvocableHandlerMethod method = getFooInvocableHandlerMethod();
		String s = new ObjectMapper().writeValueAsString(new Foo().setText(TEXT));
		method.invoke(new GenericMessage<>(s.getBytes(StandardCharsets.UTF_8)));
	}

	public void onMessageWithFoo(@ResolvableType Foo payload) {
		assertThat(payload.getText()).isEqualTo(TEXT);
	}

	@Test
	void stringToFooForceJson() throws Exception {
		final InvocableHandlerMethod method = getFooInvocableHandlerMethod();
		final String s = new ObjectMapper().writeValueAsString(new Foo().setText(TEXT));
		final Map<String, Object> contentType = Map.of(MessageHeaders.CONTENT_TYPE, "application/xml");
		// No converters can process xml media type, so will cause MessageConversionException.
		assertThatThrownBy(() ->
				method.invoke(new GenericMessage<>(s, contentType)))
				.isExactlyInstanceOf(MessageConversionException.class);
		// Ignore the header, and force using own converter.
		InvocableHandlerMethod forceMethod = getForceJsonFooInvocableHandlerMethod();
		forceMethod.invoke(new GenericMessage<>(s, contentType));
	}

	public void onForceJsonMessageWithFoo(@ResolvableType(force = true) Foo payload) {
		assertThat(payload.getText()).isEqualTo(TEXT);
	}

	@NotNull
	private InvocableHandlerMethod getFooInvocableHandlerMethod() throws NoSuchMethodException {
		MessageHandlerMethodFactory factory = getMessageHandlerMethodFactory();
		return factory.createInvocableHandlerMethod(this, getClass().getDeclaredMethod(
				"onMessageWithFoo", Foo.class));
	}

	@NotNull
	private InvocableHandlerMethod getForceJsonFooInvocableHandlerMethod() throws NoSuchMethodException {
		MessageHandlerMethodFactory factory = getMessageHandlerMethodFactory();
		return factory.createInvocableHandlerMethod(this, getClass().getDeclaredMethod(
				"onForceJsonMessageWithFoo", Foo.class));
	}

	@SuppressWarnings("rawtypes")
	private MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
		KafkaListenerAnnotationBeanPostProcessor bpp = new KafkaListenerAnnotationBeanPostProcessor<>();
		beanPostProcessorConfig(bpp);
		return bpp.getMessageHandlerMethodFactory();
	}

	@SuppressWarnings("rawtypes")
	private void beanPostProcessorConfig(KafkaListenerAnnotationBeanPostProcessor processor) {
		processor.setBeanFactory(new KafkaBeanFactory());
		try {
			processor.afterSingletonsInstantiated();
		} catch (NoSuchBeanDefinitionException ignore) {
		}
	}

	@SuppressWarnings("unchecked")
	static class KafkaBeanFactory extends DefaultListableBeanFactory {
		@Override
		public <T> Map<String, T> getBeansOfType(Class<T> type) throws BeansException {
			return new HashMap<>() {
				{
					if (KafkaListenerConfigurer.class.equals(type)) {
						put("bean", (T) new ResolvableTypeConfig());
					}
				}
			};
		}
	}

	static class ResolvableTypeConfig implements KafkaListenerConfigurer {
		public CompositeMessageConverter createMessageConverter() {
			List<MessageConverter> converters = new ArrayList<>();
			converters.add(new StringMessageConverter());
			converters.add(new ByteArrayMessageConverter());
			converters.add(createJacksonConverter());
			return new CompositeMessageConverter(converters);
		}

		protected MappingJackson2MessageConverter createJacksonConverter() {
			MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
			converter.setContentTypeResolver(new DefaultContentTypeResolver());
			return converter;
		}

		@Override
		public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
			registrar.setCustomMethodArgumentResolvers(new ResolvableTypeResolver(createMessageConverter()));
		}
	}

	static class Foo {
		private String text;

		public String getText() {
			return text;
		}

		public Foo setText(String text) {
			this.text = text;
			return this;
		}
	}

}
