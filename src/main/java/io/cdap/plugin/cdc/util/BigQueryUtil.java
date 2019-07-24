/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.cdc.util;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 *
 */
public final class BigQueryUtil {
    private static final Map<Schema.Type, Set<LegacySQLTypeName>> TYPE_MAP = ImmutableMap.<Schema.Type,
            Set<LegacySQLTypeName>>builder()
            .put(Schema.Type.INT, ImmutableSet.of(LegacySQLTypeName.INTEGER))
            .put(Schema.Type.LONG, ImmutableSet.of(LegacySQLTypeName.INTEGER))
            .put(Schema.Type.STRING, ImmutableSet.of(LegacySQLTypeName.STRING, LegacySQLTypeName.DATETIME))
            .put(Schema.Type.FLOAT, ImmutableSet.of(LegacySQLTypeName.FLOAT))
            .put(Schema.Type.DOUBLE, ImmutableSet.of(LegacySQLTypeName.FLOAT))
            .put(Schema.Type.BOOLEAN, ImmutableSet.of(LegacySQLTypeName.BOOLEAN))
            .put(Schema.Type.BYTES, ImmutableSet.of(LegacySQLTypeName.BYTES))
            .build();

    private static final Map<Schema.LogicalType, LegacySQLTypeName> LOGICAL_TYPE_MAP = ImmutableMap.of(
            Schema.LogicalType.DATE, LegacySQLTypeName.DATE,
            Schema.LogicalType.TIME_MILLIS, LegacySQLTypeName.TIME,
            Schema.LogicalType.TIME_MICROS, LegacySQLTypeName.TIME,
            Schema.LogicalType.TIMESTAMP_MILLIS, LegacySQLTypeName.TIMESTAMP,
            Schema.LogicalType.TIMESTAMP_MICROS, LegacySQLTypeName.TIMESTAMP
    );

    /**
     * Gets non nullable type from provided schema.
     *
     * @param schema schema to be used
     * @return non-nullable {@link Schema}
     */
    public static Schema getNonNullableSchema(Schema schema) {
        return schema.isNullable() ? schema.getNonNullable() : schema;
    }



    /**
     * Get BigQuery Table.
     *
     * @param serviceAccountFilePath service account file path
     * @param project                BigQuery project ID
     * @param dataset                dataset for the BigQuery table
     * @param table                  BigQuery table
     * @return returns BigQuery table
     * @throws IOException if not able to load credentials
     */
    @Nullable
    public static Table getBigQueryTable(@Nullable String serviceAccountFilePath, String project,
                                         String dataset, String table) throws IOException {
        BigQuery bigquery = getBigQuery(serviceAccountFilePath, project);
        TableId id = TableId.of(project, dataset, table);
        return bigquery.getTable(id);
    }

    /**
     * Get BigQuery service
     *
     * @param serviceAccountFilePath service account file path
     * @param project                BigQuery project ID
     */
    public static BigQuery getBigQuery(@Nullable String serviceAccountFilePath, String project) throws IOException {
        BigQueryOptions.Builder bigqueryBuilder = BigQueryOptions.newBuilder();
        if (serviceAccountFilePath != null) {
            bigqueryBuilder.setCredentials(GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath));
        }

        bigqueryBuilder.setProjectId(project);
        return bigqueryBuilder.build().getService();
    }

    /**
     * Validates if provided field schema matches with BigQuery table column type.
     *
     * @param bqField bigquery table field
     * @param field   schema field
     * @param dataset dataset name
     * @param table   table name
     * @throws IllegalArgumentException if schema types do not match
     */
    public static void validateFieldSchemaMatches(Field bqField, Schema.Field field, String dataset, String table) {
        // validate type of fields against BigQuery column type
        Schema fieldSchema = getNonNullableSchema(field.getSchema());
        Schema.Type type = fieldSchema.getType();

        Schema.LogicalType logicalType = fieldSchema.getLogicalType();

        // validate logical types
        if (logicalType != null) {
            if (LOGICAL_TYPE_MAP.get(logicalType) == null) {
                throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'",
                        field.getName(), logicalType));
            }
            if (LOGICAL_TYPE_MAP.get(logicalType) != bqField.getType()) {
                throw new IllegalArgumentException(
                        String.format("Field '%s' of type '%s' is not compatible with column '%s' in BigQuery table" +
                                        " '%s.%s' of type '%s'. It must be of type '%s'.",
                                field.getName(), logicalType, bqField.getName(), dataset, table,
                                bqField.getType(), bqField.getType()));
            }
            // Return once logical types are validated. This is because logical types are represented as primitive types
            // internally.
            return;
        }

        if (TYPE_MAP.get(type) == null) {
            throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'",
                    field.getName(), type));
        }

        if (!TYPE_MAP.get(type).contains(bqField.getType())) {
            throw new IllegalArgumentException(
                    String.format("Field '%s' of type '%s' is not compatible with column '%s' in BigQuery table" +
                                    " '%s.%s' of type '%s'. It must be of type '%s'.",
                            field.getName(), type, bqField.getName(), dataset, table, bqField.getType(),
                            bqField.getType()));
        }
    }

    /**
     * Get difference of schema fields and big query table fields. The operation is equivalent to
     * (Names of schema fields - Names of bigQuery table fields).
     *
     * @param schemaFields schema fields
     * @param bqFields     bigquery table fields
     * @return list of remaining field names
     */
    public static List<String> getSchemaMinusBqFields(List<Schema.Field> schemaFields, FieldList bqFields) {
        List<String> diff = new ArrayList<>();

        for (Schema.Field field : schemaFields) {
            diff.add(field.getName());
        }

        for (Field field : bqFields) {
            diff.remove(field.getName());
        }
        return diff;
    }

    /**
     * Get difference of big query table fields and schema fields. The operation is equivalent to
     * (Names of bigQuery table fields - Names of schema fields).
     *
     * @param bqFields     bigquery table fields
     * @param schemaFields schema fields
     * @return list of remaining field names
     */
    public static List<String> getBqFieldsMinusSchema(FieldList bqFields, List<Schema.Field> schemaFields) {
        List<String> diff = new ArrayList<>();

        for (Field field : bqFields) {
            diff.add(field.getName());
        }

        for (Schema.Field field : schemaFields) {
            diff.remove(field.getName());
        }
        return diff;
    }

}