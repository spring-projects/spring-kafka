package com.common;

public class Foo1 {

  public String bar;

  public Foo1() {}

  public Foo1(String bar) { this.bar = bar; }

  public String getFoo() { return this.bar; }

  public void setFoo(String bar) { this.bar = bar; }

  @Override
  public String toString() { return "Foo1 [bar=" + this.bar + "]"; }
}
