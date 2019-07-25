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

import com.typesafe.config.ConfigValueFactory;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.metastore.iceberg.IcebergBaseTest;
import org.apache.drill.metastore.iceberg.config.IcebergConfigConstants;
import org.apache.drill.metastore.iceberg.exceptions.IcebergMetastoreException;
import org.apache.iceberg.Table;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestExpirationHandler extends IcebergBaseTest {

  @Test
  public void testConfigEmpty() {
    ExpirationHandler expirationHandler = new ExpirationHandler(DrillConfig.create(), baseHadoopConfig());
    assertEquals(0, expirationHandler.expirationPeriod());
  }

  @Test
  public void testConfigOneUnit() {
    DrillConfig config = new DrillConfig(DrillConfig.create()
      .withValue(IcebergConfigConstants.EXPIRATION_PERIOD + ".hours",
        ConfigValueFactory.fromAnyRef(5)));
    ExpirationHandler expirationHandler = new ExpirationHandler(config, baseHadoopConfig());
    assertEquals(TimeUnit.HOURS.toMillis(5), expirationHandler.expirationPeriod());
  }

  @Test
  public void testConfigSeveralUnits() {
    DrillConfig config = new DrillConfig(DrillConfig.create()
      .withValue(IcebergConfigConstants.EXPIRATION_PERIOD + ".hours",
        ConfigValueFactory.fromAnyRef(5))
      .withValue(IcebergConfigConstants.EXPIRATION_PERIOD + ".minutes",
        ConfigValueFactory.fromAnyRef(10)));
    ExpirationHandler expirationHandler = new ExpirationHandler(config, baseHadoopConfig());
    assertEquals(TimeUnit.HOURS.toMillis(5) + TimeUnit.MINUTES.toMillis(10),
      expirationHandler.expirationPeriod());
  }

  @Test
  public void testConfigNegativeValue() {
    DrillConfig config = new DrillConfig(DrillConfig.create()
      .withValue(IcebergConfigConstants.EXPIRATION_PERIOD + ".hours",
        ConfigValueFactory.fromAnyRef(-5)));
    ExpirationHandler expirationHandler = new ExpirationHandler(config, baseHadoopConfig());
    assertEquals(TimeUnit.HOURS.toMillis(-5), expirationHandler.expirationPeriod());
  }

  @Test
  public void testConfigIncorrectUnit() {
    DrillConfig config = new DrillConfig(DrillConfig.create()
      .withValue(IcebergConfigConstants.EXPIRATION_PERIOD + ".hour",
        ConfigValueFactory.fromAnyRef(5)));

    thrown.expect(IcebergMetastoreException.class);
    new ExpirationHandler(config, baseHadoopConfig());
  }

  @Test
  public void testConfigIncorrectValue() {
    DrillConfig config = new DrillConfig(DrillConfig.create()
      .withValue(IcebergConfigConstants.EXPIRATION_PERIOD + ".hours",
        ConfigValueFactory.fromAnyRef("abc")));

    thrown.expect(IcebergMetastoreException.class);
    new ExpirationHandler(config, baseHadoopConfig());
  }

  @Test
  public void testExpireZeroExpirationPeriod() {
    DrillConfig config = new DrillConfig(DrillConfig.create()
      .withValue(IcebergConfigConstants.EXPIRATION_PERIOD + ".millis",
        ConfigValueFactory.fromAnyRef(0)));

    ExpirationHandler expirationHandler = new ExpirationHandler(config, baseHadoopConfig());
    Table table = mock(Table.class);
    assertFalse(expirationHandler.expire(table));
  }

  @Test
  public void testExpireNegativeExpirationPeriod() {
    DrillConfig config = new DrillConfig(DrillConfig.create()
      .withValue(IcebergConfigConstants.EXPIRATION_PERIOD + ".millis",
        ConfigValueFactory.fromAnyRef(-10)));

    ExpirationHandler expirationHandler = new ExpirationHandler(config, baseHadoopConfig());
    Table table = mock(Table.class);
    assertFalse(expirationHandler.expire(table));
  }

  @Test
  public void testExpireFirstTime() {
    DrillConfig config = new DrillConfig(DrillConfig.create()
      .withValue(IcebergConfigConstants.EXPIRATION_PERIOD + ".millis",
        ConfigValueFactory.fromAnyRef(1)));

    ExpirationHandler expirationHandler = new ExpirationHandler(config, baseHadoopConfig());

    Table table = mock(Table.class);
    when(table.location()).thenReturn("/tmp/table");

    assertFalse(expirationHandler.expire(table));
  }

  @Test
  public void testExpireBefore() {
    DrillConfig config = new DrillConfig(DrillConfig.create()
      .withValue(IcebergConfigConstants.EXPIRATION_PERIOD + ".days",
        ConfigValueFactory.fromAnyRef(1)));

    ExpirationHandler expirationHandler = new ExpirationHandler(config, baseHadoopConfig());

    Table table = mock(Table.class);
    when(table.location()).thenReturn("/tmp/table");

    assertFalse(expirationHandler.expire(table));
    assertFalse(expirationHandler.expire(table));
  }
}
