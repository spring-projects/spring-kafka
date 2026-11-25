/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package com.common;

/**
 * @author Gary Russell
 * @since 2.2.1
 *
 */
public class Bar1 {

	public String bar;

	public Bar1() {
	}

	public Bar1(String bar) {
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
		return "Bar1 [bar=" + this.bar + "]";
	}

}
