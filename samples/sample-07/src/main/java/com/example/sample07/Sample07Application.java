/*
 * Copyright 2022-2024 the original author or authors.
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

package com.example.sample07;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * New consumer rebalance protocol sample which purpose is only to be used in the test assertions.
 * See unit tests for this project for more information.
 *
 * @author Sanghyeok An.
 *
 * @since 3.3
 */

@SpringBootApplication
public class Sample07Application {

	public static void main(String[] args) {
		SpringApplication.run(Sample07Application.class, args);
	}

}
