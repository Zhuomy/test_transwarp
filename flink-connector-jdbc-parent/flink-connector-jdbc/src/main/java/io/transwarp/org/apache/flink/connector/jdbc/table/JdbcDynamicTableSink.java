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

package io.transwarp.org.apache.flink.connector.jdbc.table;

import io.transwarp.org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import io.transwarp.org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import io.transwarp.org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import io.transwarp.org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link DynamicTableSink} for JDBC.
 */
@Internal
public class JdbcDynamicTableSink implements DynamicTableSink {

  private final InternalJdbcConnectionOptions jdbcOptions;
  private final JdbcExecutionOptions executionOptions;
  private final JdbcDmlOptions dmlOptions;
  private final DataType physicalRowDataType;
  private final String dialectName;

  public JdbcDynamicTableSink(
    InternalJdbcConnectionOptions jdbcOptions,
    JdbcExecutionOptions executionOptions,
    JdbcDmlOptions dmlOptions,
    DataType physicalRowDataType) {
    this.jdbcOptions = jdbcOptions;
    this.executionOptions = executionOptions;
    this.dmlOptions = dmlOptions;
    this.physicalRowDataType = physicalRowDataType;
    this.dialectName = dmlOptions.getDialect().dialectName();
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    validatePrimaryKey(requestedMode);
    return ChangelogMode.newBuilder()
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.DELETE)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build();
  }

  private void validatePrimaryKey(ChangelogMode requestedMode) {
    checkState(
      ChangelogMode.insertOnly().equals(requestedMode)
        || dmlOptions.getKeyFields().isPresent(),
      "please declare primary key for sink table when query contains update/delete record.");
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    final TypeInformation<RowData> rowDataTypeInformation =
      context.createTypeInformation(physicalRowDataType);
    final JdbcOutputFormatBuilder builder = new JdbcOutputFormatBuilder();

    builder.setJdbcOptions(jdbcOptions);
    builder.setJdbcDmlOptions(dmlOptions);
    builder.setJdbcExecutionOptions(executionOptions);
    builder.setRowDataTypeInfo(rowDataTypeInformation);
    builder.setFieldDataTypes(
      DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]));
    return SinkFunctionProvider.of(
      new GenericJdbcSinkFunction<>(builder.build()), jdbcOptions.getParallelism());
  }

  @Override
  public DynamicTableSink copy() {
    return new JdbcDynamicTableSink(
      jdbcOptions, executionOptions, dmlOptions, physicalRowDataType);
  }

  @Override
  public String asSummaryString() {
    return "JDBC:" + dialectName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JdbcDynamicTableSink)) {
      return false;
    }
    JdbcDynamicTableSink that = (JdbcDynamicTableSink) o;
    return Objects.equals(jdbcOptions, that.jdbcOptions)
      && Objects.equals(executionOptions, that.executionOptions)
      && Objects.equals(dmlOptions, that.dmlOptions)
      && Objects.equals(physicalRowDataType, that.physicalRowDataType)
      && Objects.equals(dialectName, that.dialectName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
      jdbcOptions, executionOptions, dmlOptions, physicalRowDataType, dialectName);
  }
}
