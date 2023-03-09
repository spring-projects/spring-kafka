package com.common;

public class Bar1 {

  public String bar;

  public Bar() {}

  public Bar1(String bar) { this.bar = bar; }

  public String getBar() { return this.bar; }

  public void setBar(String bar) { this.bar = bar; }

  @Override
  public String toString() { return "Bar1 [bar=" + this.bar + "]"; }
}
