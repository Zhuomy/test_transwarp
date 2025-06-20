/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.transwarp.org.apache.flink.connector.jdbc.internal.options;

import lombok.Getter;
import io.transwarp.org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * JDBC sink DML options.
 */
public class JdbcDmlOptions extends JdbcTypedQueryOptions {

  private static final long serialVersionUID = 1L;

  @Getter
  private final String[] fieldNames;
  @Nullable
  private final String[] keyFields;
  @Getter
  private final String tableName;
  @Getter
  private final JdbcDialect dialect;

  public static JdbcDmlOptionsBuilder builder() {
    return new JdbcDmlOptionsBuilder();
  }

  private JdbcDmlOptions(
    String tableName,
    JdbcDialect dialect,
    String[] fieldNames,
    int[] fieldTypes,
    String[] keyFields) {
    super(fieldTypes);
    this.tableName = Preconditions.checkNotNull(tableName, "table is empty");
    this.dialect = Preconditions.checkNotNull(dialect, "dialect is empty");
    this.fieldNames = Preconditions.checkNotNull(fieldNames, "field names is empty");
    this.keyFields = keyFields;
  }

    public Optional<String[]> getKeyFields() {
    return Optional.ofNullable(keyFields);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JdbcDmlOptions that = (JdbcDmlOptions) o;
    return Arrays.equals(fieldNames, that.fieldNames)
      && Arrays.equals(keyFields, that.keyFields)
      && Objects.equals(tableName, that.tableName)
      && Objects.equals(dialect.dialectName(), that.dialect.dialectName());
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(tableName, dialect.dialectName());
    result = 31 * result + Arrays.hashCode(fieldNames);
    result = 31 * result + Arrays.hashCode(keyFields);
    return result;
  }

  /**
   * Builder for {@link JdbcDmlOptions}.
   */
  public static class JdbcDmlOptionsBuilder
    extends JdbcUpdateQueryOptionsBuilder<JdbcDmlOptionsBuilder> {
    private String tableName;
    private String[] fieldNames;
    private String[] keyFields;
    private JdbcDialect dialect;

    @Override
    protected JdbcDmlOptionsBuilder self() {
      return this;
    }

    public JdbcDmlOptionsBuilder withFieldNames(String field, String... fieldNames) {
      this.fieldNames = concat(field, fieldNames);
      return this;
    }

    public JdbcDmlOptionsBuilder withFieldNames(String[] fieldNames) {
      this.fieldNames = fieldNames;
      return this;
    }

    public JdbcDmlOptionsBuilder withKeyFields(String keyField, String... keyFields) {
      this.keyFields = concat(keyField, keyFields);
      return this;
    }

    public JdbcDmlOptionsBuilder withKeyFields(String[] keyFields) {
      this.keyFields = keyFields;
      return this;
    }

    public JdbcDmlOptionsBuilder withTableName(String tableName) {
      this.tableName = tableName;
      return self();
    }

    public JdbcDmlOptionsBuilder withDialect(JdbcDialect dialect) {
      this.dialect = dialect;
      return self();
    }

    public JdbcDmlOptions build() {
      return new JdbcDmlOptions(tableName, dialect, fieldNames, fieldTypes, keyFields);
    }

    static String[] concat(String first, String... next) {
      if (next == null || next.length == 0) {
        return new String[]{first};
      } else {
        return Stream.concat(Stream.of(new String[]{first}), Stream.of(next))
          .toArray(String[]::new);
      }
    }
  }
}
