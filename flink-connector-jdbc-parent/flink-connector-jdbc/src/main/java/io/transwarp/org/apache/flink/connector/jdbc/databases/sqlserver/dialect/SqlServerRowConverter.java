/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.transwarp.org.apache.flink.connector.jdbc.databases.sqlserver.dialect;

import io.transwarp.org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * MsSql.
 */
@Internal
public class SqlServerRowConverter extends AbstractJdbcRowConverter {

  private static final long serialVersionUID = 1L;

  @Override
  public String converterName() {
    return "SqlServer";
  }

  public SqlServerRowConverter(RowType rowType) {
    super(rowType);
  }

  @Override
  protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
    switch (type.getTypeRoot()) {
      case TINYINT:
        return val -> ((Short) val).byteValue();
      default:
        return super.createInternalConverter(type);
    }
  }
}
