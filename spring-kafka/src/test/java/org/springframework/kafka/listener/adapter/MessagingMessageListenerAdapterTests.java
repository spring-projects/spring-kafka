/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener.adapter;

import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.support.GenericMessage;

/**
 * @author Gary Russell
 * @since 1.1.2
 *
 */
public class MessagingMessageListenerAdapterTests {

	@Test
	public void testFallbackType() {
		final class MyAdapter extends MessagingMessageListenerAdapter<String, String>
				implements AcknowledgingMessageListener<String, String> {

			private MyAdapter() {
				super(null, null);
			}

			@Override
			public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
				toMessagingMessage(data, acknowledgment, null);
			}

		}
		MyAdapter adapter = new MyAdapter();
		adapter.setFallbackType(String.class);
		RecordMessageConverter converter = mock(RecordMessageConverter.class);
		ConsumerRecord<String, String> cr = new ConsumerRecord<>("foo", 1, 1L, null, null);
		Acknowledgment ack = mock(Acknowledgment.class);
		willReturn(new GenericMessage<>("foo")).given(converter).toMessage(cr, ack, null, String.class);
		adapter.setMessageConverter(converter);
		adapter.onMessage(cr, ack);
		verify(converter).toMessage(cr, ack, null, String.class);
	}

}
