/*
 * Copyright 2018 the original author or authors.
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

package com.example;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.common.Bar2;
import com.common.Foo2;

/**
 * @author Gary Russell
 * @since 5.1
 *
 */
@Component
@KafkaListener(id = "multiGroup", topics = { "foos", "bars" })
public class MultiMethods {

	@KafkaHandler
	public void foo(Foo2 foo) {
		System.out.println("Received: " + foo);
	}

	@KafkaHandler
	public void bar(Bar2 bar) {
		System.out.println("Received: " + bar);
	}

	@KafkaHandler(isDefault = true)
	public void unknown(Object object) {
		System.out.println("Received unknown: " + object);
	}

}
