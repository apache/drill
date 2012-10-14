package org.apache.drill.plan.ast;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created with IntelliJ IDEA. User: tdunning Date: 10/12/12 Time: 7:41 PM To change this template
 * use File | Settings | File Templates.
 */
public class Plan {
  private List<Op> statements = Lists.newArrayList();

  public static Plan create(Op first) {
    Plan r = new Plan();
    return r.add(first);
  }

  public Plan add(Op next) {
      if (next != null) {
          statements.add(next);
      }
      return this;
  }

  public List<Op> getStatements() {
    return statements;
  }
}
