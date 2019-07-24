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

package io.cdap.plugin.cdc.sink;

import com.google.api.services.bigquery.model.JsonObject;
import com.google.cloud.bigquery.*;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.gson.JsonNull;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.plugin.cdc.util.BigQueryUtil;
import io.cdap.plugin.cdc.util.GCPConfig;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 *
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(BigQueryStreamingSink.NAME)
@Description("A plugin to load streaming data into BigQuery.")
public class BigQueryStreamingSink extends SparkSink<StructuredRecord> {

    private final Config config;

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryStreamingSink.class);
    static final String NAME = "BigQueryStreamingSink";
    private int counter = 0;

    public BigQueryStreamingSink(Config config) {
        this.config = config;
    }

    @Override
    public void prepareRun(SparkPluginContext context) throws Exception {

        //config.validate(context.getInputSchema());
        BigQuery bigquery;

        bigquery = BigQueryUtil.getBigQuery(config.getServiceAccountFilePath(), config.getProject());
        LOG.info(config.getServiceAccountFilePath());
        // create dataset if it does not exist
        if (bigquery.getDataset(config.getDataset()) == null) {
            try {
                bigquery.create(DatasetInfo.newBuilder(config.getDataset()).build());
            } catch (BigQueryException e) {
                throw new RuntimeException("Exception occurred while creating dataset " + config.getDataset() + ".", e);
            }
        }

        // schema validation against bigquery table schema
        validateSchema(context.getInputSchema());

        List<BigQueryTableFieldSchema> bigqueryFields = new ArrayList<>();
        ArrayList<Field> bqFields = new ArrayList<>();
        Schema schema = context.getInputSchema();
        TableId tableId = TableId.of(config.getDataset(), config.getTable());
        if (schema == null) {
            throw new NullPointerException("Schema is null");
        }
        List<Schema.Field> cdapFields = schema.getFields();
        if (cdapFields == null) {
            throw new NullPointerException("Fields is null");
        }
        for (Schema.Field field : cdapFields) {
            LegacySQLTypeName tableTypeName = getTableDataType(BigQueryUtil.getNonNullableSchema(field.getSchema()));
            BigQueryTableFieldSchema tableFieldSchema = new BigQueryTableFieldSchema()
                    .setName(field.getName())
                    .setType(tableTypeName.name())
                    .setMode(Field.Mode.NULLABLE.name());
            bigqueryFields.add(tableFieldSchema);
            Field f = Field.of(field.getName(), tableTypeName);
            bqFields.add(f);
        }

        // CREATE TABLE HERE
        com.google.cloud.bigquery.Schema schema1 = com.google.cloud.bigquery.Schema.of(bqFields);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema1);

        Table object = bigquery.getTable(tableId);
        LOG.info("Table " + tableId + " does not exist.");
        if (object == null) {
            LOG.info("Schema to be created" + tableDefinition.toString());
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
            bigquery.create(tableInfo);
            Thread.sleep(5000);
        }

    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        super.configurePipeline(pipelineConfigurer);
        Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
        //config.validate(inputSchema);
        /*try {
            pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
        } catch (IOException e) {
            throw new IllegalArgumentException("Output schema cannot be parsed.", e);
        }*/
    }

    @Override
    public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> javaRDD) {
        // maps data sets to each block of computing resources
        javaRDD.foreachPartition(structuredRecordIterator -> {
            int batchSize = config.getBatchSize();
            Boolean status = false;

            TableId tableId = TableId.of(config.getProject(), config.getDataset(), config.getTable());
            InsertAllRequest.Builder insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
            BigQuery bigquery = BigQueryUtil.getBigQuery(config.getServiceAccountFilePath(), config.getProject());

            while (structuredRecordIterator.hasNext()) {
                StructuredRecord input = structuredRecordIterator.next();
                if (input.getSchema() != null) {
                    JsonObject object = transform(input);
                    if (counter++ == batchSize) {
                        InsertAllResponse response = bigquery.insertAll(insertAllRequestBuilder.build());
                        if (response.hasErrors()) {
                            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                                LOG.info("Error: Key: {} Value: {}", entry.getKey(), entry.getValue().toString());
                            }
                        }
                        insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
                        counter = 0;
                        status = false;
                    }
                    insertAllRequestBuilder.addRow(object);
                    status = true;
                }

            }
            if (status) {
                InsertAllResponse response = bigquery.insertAll(insertAllRequestBuilder.build());
                if (response.hasErrors()) {
                    for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                        LOG.info("Error: Key: {} Value: {}", entry.getKey(), entry.getValue().toString());
                    }
                }
            }

        });
    }

    private void validateSchema(Schema inputSchema) throws IOException {
        Table table = BigQueryUtil.getBigQueryTable(config.getServiceAccountFilePath(), config.getProject(),
                config.getDataset(), config.getTable());
        if (table == null) {
            // Table does not exist, so no further validation is required.
            return;
        }
        com.google.cloud.bigquery.Schema bqSchema = table.getDefinition().getSchema();
        if (bqSchema == null) {
            // Table is created without schema, so no further validation is required.
            return;
        }

        FieldList bqFields = bqSchema.getFields();
        List<Schema.Field> outputSchemaFields = inputSchema.getFields();
        if (outputSchemaFields == null) {
            throw new NullPointerException("outputSchemaFields is null");
        }
        // Output schema should not have fields that are not present in BigQuery table.
        List<String> diff = BigQueryUtil.getSchemaMinusBqFields(outputSchemaFields, bqFields);
        if (!diff.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("The output schema does not match the BigQuery table schema for '%s.%s' table. " +
                                    "The table does not contain the '%s' column(s).",
                            config.getDataset(), config.getTable(), diff));
        }
        // validate the missing columns in output schema are nullable fields in bigquery
        List<String> remainingBQFields = BigQueryUtil.getBqFieldsMinusSchema(bqFields, outputSchemaFields);
        for (String field : remainingBQFields) {
            if (bqFields.get(field).getMode() != Field.Mode.NULLABLE) {
                throw new IllegalArgumentException(
                        String.format("The output schema does not match the BigQuery table schema for '%s.%s'. " +
                                        "The table requires column '%s', which is not in the output schema.",
                                config.getDataset(), config.getTable(), field));
            }
        }
        // Match output schema field type with bigquery column type
        Schema schema = inputSchema;
        if (schema == null) {
            throw new NullPointerException("Schema is null");
        }
        List<Schema.Field> fields = schema.getFields();
        if (fields == null) {
            throw new NullPointerException("Fields is null");
        }
        for (Schema.Field field : fields) {
            BigQueryUtil.validateFieldSchemaMatches(bqFields.get(field.getName()),
                    field, config.getDataset(), config.getTable());
        }
    }

    private LegacySQLTypeName getTableDataType(Schema schema) {
        Schema.LogicalType logicalType = schema.getLogicalType();
        if (logicalType != null) {
            switch (logicalType) {
                case DATE:
                    return LegacySQLTypeName.DATE;
                case TIME_MILLIS:
                case TIME_MICROS:
                    return LegacySQLTypeName.TIME;
                case TIMESTAMP_MILLIS:
                case TIMESTAMP_MICROS:
                    return LegacySQLTypeName.TIMESTAMP;
                default:
                    throw new IllegalStateException("Unsupported logical type " + logicalType);
            }
        }

        Schema.Type type = schema.getType();
        switch (type) {
            case INT:
            case LONG:
                return LegacySQLTypeName.INTEGER;
            case STRING:
                return LegacySQLTypeName.STRING;
            case FLOAT:
            case DOUBLE:
                return LegacySQLTypeName.FLOAT;
            case BOOLEAN:
                return LegacySQLTypeName.BOOLEAN;
            case BYTES:
                return LegacySQLTypeName.BYTES;
            default:
                throw new IllegalStateException("Unsupported type " + type);
        }
    }

    private static void decodeSimpleTypes(JsonObject json, String name, StructuredRecord input) {
        Object object = input.get(name);
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneId.of("UTC"));
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");

        Schema inputSchema = input.getSchema();
        Schema.Field field = inputSchema.getField(name);
        if (field == null) {
            throw new NullPointerException("Field is null");
        }
        Schema schema = BigQueryUtil.getNonNullableSchema(field.getSchema());
        if (object == null) {
            return;
        }
        Schema.LogicalType logicalType = schema.getLogicalType();
        if (logicalType != null) {
            switch (logicalType) {
                case DATE:
                    LocalDate date = input.getDate(name);
                    if (date == null) {
                        throw new NullPointerException("Date is null");
                    }
                    json.set(name, date.toString());
                    break;
                case TIME_MILLIS:
                case TIME_MICROS:
                    LocalTime time = input.getTime(name);
                    if (time == null) {
                        throw new NullPointerException("Time is null");
                    }
                    json.set(name, timeFormatter.format(time));
                    break;
                case TIMESTAMP_MILLIS:
                case TIMESTAMP_MICROS:
                    //timestamp for json input should be in this format yyyy-MM-dd HH:mm:ss.SSSSSS
                    ZonedDateTime timestamp = input.getTimestamp(name);
                    if (timestamp == null) {
                        throw new NullPointerException("Timestamp is null");
                    }
                    json.set(name, dtf.format(timestamp));
                    break;
                default:
                    throw new IllegalStateException(String.format("Unsupported logical type %s", logicalType));
            }
            return;
        }
        Schema.Type type = schema.getType();
        switch (type) {
            case NULL:
                json.set(name, JsonNull.INSTANCE); // nothing much to do here.
                break;
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                json.set(name, object);
                break;
            /*TODO: add support for RECORD type.
                case RECORD:
                json.set(name, object);
                break;*/
            case BOOLEAN:
                json.set(name, object);
                break;
            case STRING:
                json.set(name, object.toString());
                break;
            case BYTES:
                json.set(name, Base64.getEncoder().encodeToString((byte[]) object));
                break;
            default:
                throw new IllegalStateException(String.format("Unsupported type %s", type));
        }
    }

    private JsonObject transform(StructuredRecord input) {
        JsonObject object = new JsonObject();

        Schema schema = input.getSchema();
        if (schema == null) {
            throw new NullPointerException("Schema is null");
        }

        List<Schema.Field> fields = schema.getFields();
        if (fields == null) {
            throw new NullPointerException("Fields is null");
        }
        for (Schema.Field recordField : fields) {
            // From all the fields in input record, decode only those fields that are present in output schema.
            decodeSimpleTypes(object, recordField.getName(), input);
        }
        return object;
    }

    /**
     *
     */
    public static class Config extends GCPConfig {

        @Macro
        @Description("The dataset to write to. A dataset is contained within a specific project. "
                + "Datasets are top-level containers that are used to organize and control access to tables and views.")
        private String dataset;

        @Macro
        @Description("The table to write to. A table contains individual records organized in rows. "
                + "Each record is composed of columns (also called fields). "
                + "Every table is defined by a schema that describes the column names, data types, and other " +
                "information.")
        private String table;

        @Macro
        @Description("The number of records inserted into BigQuery at a time in a streaming mode.")
        private String batchsize;

        public Config(String dataset, String table,
                      String batchsize) {
            this.dataset = dataset;
            this.table = table;
            this.batchsize = batchsize;
        }

        public String getDataset() {
            return dataset;
        }

        public String getTable() {
            return table;
        }

        public int getBatchSize() {
            return Integer.parseInt(batchsize);
        }

        /**
         * @return the schema of the dataset
         * @throws IllegalArgumentException if the schema is null or invalid
         */
        /*public Schema getSchema() {
            if (schema == null) {
                throw new IllegalArgumentException("Schema must be specified.");
            }
            try {
                return Schema.parseJson(schema);
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
            }
        }*/

        /**
         * Verifies if output schema only contains simple types. It also verifies if all the output schema fields are
         * present in input schema.
         *
         * @param inputSchema input schema to bigquery sink
         */
        /*public void validate(@Nullable Schema inputSchema) {

            if (!containsMacro("schema")) {
                Schema outputSchema = getSchema();
                for (Schema.Field field : outputSchema.getFields()) {
                    // check if the required fields are present in the input schema.
                    if (!field.getSchema().isNullable() && inputSchema != null && inputSchema.getField(field.getName())
                     == null) {
                        throw new IllegalArgumentException(String.format("Required output field '%s' is not present in
                        input schema.", field.getName()));
                    }

                    Schema fieldSchema = BigQueryUtil.getNonNullableSchema(field.getSchema());

                    if (!fieldSchema.getType().isSimpleType()) {
                        throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'.",
                                field.getName(), fieldSchema.getType()));
                    }
                }
            }
        }*/
    }
}
