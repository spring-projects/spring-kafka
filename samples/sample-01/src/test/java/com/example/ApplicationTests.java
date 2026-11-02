/*
 * Copyright 2018-present the original author or authors.
 */

package com.example;

import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestPropertySource(properties = "spring.profiles.active=test")
public class ApplicationTests {

	@Test
	public void contextLoads() {
	}

}
