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

package org.springframework.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.GenericMessage;

/**
 * @author Gary Russell
 * @since 2.8
 *
 */
public class HeaderMethodArgumentResolverTests {

	@SuppressWarnings("rawtypes")
	@Test
	void bytesToNumbers() throws Exception {
		KafkaListenerAnnotationBeanPostProcessor bpp = new KafkaListenerAnnotationBeanPostProcessor<>();
		MessageHandlerMethodFactory factory = bpp.getMessageHandlerMethodFactory();
		InvocableHandlerMethod method = factory.createInvocableHandlerMethod(this, getClass().getDeclaredMethod(
				"numbers", String.class,
				long.class, int.class, short.class, byte.class, Long.class, Integer.class, Short.class, Byte.class));
		method.invoke(new GenericMessage<>("foo", Map.of(
				"l1", ByteBuffer.allocate(8).putLong(1L).array(),
				"i1", ByteBuffer.allocate(4).putInt(2).array(),
				"s1", ByteBuffer.allocate(2).putShort((short) 3).array(),
				"b1", ByteBuffer.allocate(1).put((byte) 4).array(),
				"l2", ByteBuffer.allocate(8).putLong(5L).array(),
				"i2", ByteBuffer.allocate(4).putInt(6).array(),
				"s2", ByteBuffer.allocate(2).putShort((short) 7).array(),
				"b2", ByteBuffer.allocate(1).put((byte) 8).array())));
	}

	public void numbers(String payload,
			@Header("l1") long l1, @Header("i1") int i1, @Header("s1") short s1, @Header("b1") byte b1,
			@Header("l2") Long l2, @Header("i2") Integer i2, @Header("s2") Short s2, @Header("b2") Byte b2) {

		assertThat(l1).isEqualTo(1L);
		assertThat(i1).isEqualTo(2);
		assertThat(s1).isEqualTo((short) 3);
		assertThat(b1).isEqualTo((byte) 4);
		assertThat(l2).isEqualTo(5L);
		assertThat(i2).isEqualTo(6);
		assertThat(s2).isEqualTo(Short.valueOf((short) 7));
		assertThat(b2).isEqualTo(Byte.valueOf((byte) 8));
	}

}
