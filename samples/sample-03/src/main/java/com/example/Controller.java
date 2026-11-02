/*
 * Copyright 2018-present the original author or authors.
 */

package com.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.common.Foo1;

/**
 * @author Gary Russell
 * @since 2.2.1
 */
@RestController
public class Controller {

	@Autowired
	private KafkaTemplate<Object, Object> template;

	@PostMapping(path = "/send/foos/{what}")
	public void sendFoo(@PathVariable String what) {
		this.template.executeInTransaction(kafkaTemplate -> {
			StringUtils.commaDelimitedListToSet(what).stream()
				.map(s -> new Foo1(s))
				.forEach(foo -> kafkaTemplate.send("topic2", foo));
			return null;
		});
	}

}
