/*
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
package org.apache.drill.metastore.iceberg.operate;

import org.apache.drill.metastore.expressions.FilterExpression;
import org.apache.drill.metastore.iceberg.MetastoreContext;
import org.apache.drill.metastore.iceberg.transform.OperationTransformer;
import org.apache.drill.metastore.operate.AbstractModify;
import org.apache.drill.metastore.operate.MetadataTypeValidator;
import org.apache.drill.metastore.operate.Modify;
import org.apache.iceberg.Transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link Modify} interface based on {@link AbstractModify} parent class.
 * Modifies information in Iceberg table based on given overwrite or delete operations.
 * Executes given operations in one transaction.
 *
 * @param <T> Metastore component unit type
 */
public class IcebergModify<T> extends AbstractModify<T> {

  private final MetastoreContext<T> context;

  public IcebergModify(MetadataTypeValidator metadataTypeValidator, MetastoreContext<T> context) {
    super(metadataTypeValidator);
    this.context = context;
  }

  @Override
  public void execute() {
    OperationTransformer<T> transformer = context.transformer().operation();
    List<IcebergOperation> operations = new ArrayList<>(transformer.toOverwrite(overwriteUnits));
    operations.addAll(transformer.toDelete(deletes));

    if (operations.isEmpty()) {
      return;
    }

    executeOperations(operations);
  }

  @Override
  public void purge() {
    executeOperations(Collections.singletonList(
      context.transformer().operation().toDelete((FilterExpression) null)));
  }

  private void executeOperations(List<IcebergOperation> operations) {
    Transaction transaction = context.table().newTransaction();
    operations.forEach(op -> op.add(transaction));
    transaction.commitTransaction();

    // expiration process should not intervene with data modification operations
    // if expiration fails, will attempt to expire the next time
    context.expirationHandler().expireQuietly();
  }
}
