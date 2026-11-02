/*
 * Copyright 2018-present the original author or authors.
 */

package com.common;

/**
 * @author Gary Russell
 * @since 2.2.1
 *
 */
public class Bar2 {

	public String bar;

	public Bar2() {
	}

	public Bar2(String bar) {
		this.bar = bar;
	}

	public String getBar() {
		return this.bar;
	}

	public void setBar(String bar) {
		this.bar = bar;
	}

	@Override
	public String toString() {
		return "Bar2 [bar=" + this.bar + "]";
	}

}
