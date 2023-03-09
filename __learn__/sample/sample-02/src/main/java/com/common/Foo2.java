package com.common;

public class Foo2 {

  public String bar;

  public Foo2() {}

  public Foo2(String bar) { this.bar = bar; }

  public String getFoo() { return this.bar; }

  public void setFoo(String bar) { this.bar = bar; }

  @Override
  public String toString() { return "Foo2 [bar=" + this.bar + "]"; }
}
