/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.fragment;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;

import com.google.common.collect.Lists;

public class Fragment implements Iterable<Fragment.ExchangeFragmentPair> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Fragment.class);

  private PhysicalOperator root;
  private Exchange sendingExchange;
  private final List<ExchangeFragmentPair> receivingExchangePairs = Lists.newLinkedList();
  private Stats stats = new Stats();

  public void addOperator(PhysicalOperator o) {
    if (root == null) {
      root = o;
    }
  }

  public void addSendExchange(Exchange e) throws FragmentSetupException{
    if (sendingExchange != null) {
      throw new FragmentSetupException("Fragment was trying to add a second SendExchange.  ");
    }
    addOperator(e);
    sendingExchange = e;
  }

  public void addReceiveExchange(Exchange e, Fragment fragment) {
    this.receivingExchangePairs.add(new ExchangeFragmentPair(e, fragment));
  }

  @Override
  public Iterator<ExchangeFragmentPair> iterator() {
    return this.receivingExchangePairs.iterator();
  }

  public List<ExchangeFragmentPair> getReceivingExchangePairs() {
    return receivingExchangePairs;
  }

  public PhysicalOperator getRoot() {
    return root;
  }

  public Exchange getSendingExchange() {
    return sendingExchange;
  }

//  public <T, V> T accept(FragmentVisitor<T, V> visitor, V extra) {
//    return visitor.visit(this, extra);
//  }

  public Stats getStats() {
    return stats;
  }

  public class ExchangeFragmentPair {
    private Exchange exchange;
    private Fragment node;

    public ExchangeFragmentPair(Exchange exchange, Fragment node) {
      super();
      this.exchange = exchange;
      this.node = node;
    }

    public Exchange getExchange() {
      return exchange;
    }

    public Fragment getNode() {
      return node;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((exchange == null) ? 0 : exchange.hashCode());
      result = prime * result + ((node == null) ? 0 : node.hashCode());
      return result;
    }

    @Override
    public String toString() {
      return "ExchangeFragmentPair [exchange=" + exchange + "]";
    }

  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((receivingExchangePairs == null) ? 0 : receivingExchangePairs.hashCode());
    result = prime * result + ((root == null) ? 0 : root.hashCode());
    result = prime * result + ((sendingExchange == null) ? 0 : sendingExchange.hashCode());
    result = prime * result + ((stats == null) ? 0 : stats.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Fragment other = (Fragment) obj;
    if (receivingExchangePairs == null) {
      if (other.receivingExchangePairs != null) {
        return false;
      }
    } else if (!receivingExchangePairs.equals(other.receivingExchangePairs)) {
      return false;
    }
    if (root == null) {
      if (other.root != null) {
        return false;
      }
    } else if (!root.equals(other.root)) {
      return false;
    }
    if (sendingExchange == null) {
      if (other.sendingExchange != null) {
        return false;
      }
    } else if (!sendingExchange.equals(other.sendingExchange)) {
      return false;
    }
    if (stats == null) {
      if (other.stats != null) {
        return false;
      }
    } else if (!stats.equals(other.stats)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "FragmentNode [root=" + root + ", sendingExchange=" + sendingExchange + ", receivingExchangePairs="
        + receivingExchangePairs + ", stats=" + stats + "]";
  }

}
