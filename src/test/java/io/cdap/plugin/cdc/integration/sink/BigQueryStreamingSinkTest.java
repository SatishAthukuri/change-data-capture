package io.cdap.plugin.cdc.integration.sink;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.mock.spark.streaming.MockSink;
import io.cdap.cdap.etl.mock.spark.streaming.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.test.SparkManager;
import io.cdap.plugin.cdc.integration.BigQueryStreamingSinkTestBase;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.awaitility.core.ConditionFactory;
import org.junit.*;
import org.junit.rules.TestName;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BigQueryStreamingSinkTest extends BigQueryStreamingSinkTestBase {
    private static final String PLUGIN_NAME = "BigQueryStreamingSink";
    private static final String APP_NAME = BigQueryStreamingSinkTest.class.getSimpleName();
    private static final String PROJECT
            = "datafusionwhitelisted";
    private static final String SERVICE_ACCOUNT_FILE_PATH
            = "/Users/satishathukuri/Downloads/key.json";
    private static final String DATASET = "cdap";
    private static final String TABLENAME = "cdapsqlbqsink";

    private static final ConditionFactory AWAIT_PROGRAM = Awaitility.await()
            .atMost(3, TimeUnit.MINUTES)
            .pollInterval(Duration.ONE_SECOND);
    @Rule
    public TestName testName = new TestName();

    private String dbTableName;
    private SparkManager programManager;
    private ETLPlugin sinkConfig;
    private BigQuery bigQuery;

    @Before
    public void setUp() throws Exception {
        Assume.assumeNotNull(PROJECT);
        bigQuery = BigQueryOptions.getDefaultInstance().getService();
        TableId tableId = TableId.of(DATASET, TABLENAME);
        Table table = bigQuery.getTable(tableId);
        com.google.cloud.bigquery.Schema schema = table.getDefinition().getSchema();
        Map<String, String> props = ImmutableMap.<String, String>builder()
                .put("project", PROJECT)
                .put("dataset", DATASET)
                .put("tableName", TABLENAME)
                .put("serviceFilePath", SERVICE_ACCOUNT_FILE_PATH)
                .put("referenceName", "BigQueryStreamingSink")
                .build();
        sinkConfig = new ETLPlugin(PLUGIN_NAME, SparkSink.PLUGIN_TYPE, props);

//        TableId tableId = TableId.of(DATASET, TABLENAME);
//        Table table = bigQuery.getTable(tableId);
//        Logger LOG = LoggerFactory.getLogger(BigQueryStreamingSinkTest.class);
//        LOG.info("I'm here" + table.toString());
    }

    @After
    @Override
    public void afterTest() throws Exception {
        if (programManager != null) {
            programManager.stop();
            programManager.waitForStopped(10, TimeUnit.SECONDS);
            programManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.SECONDS);
        }
        super.afterTest();
        if (bigQuery != null) {
            bigQuery = null;
        }
    }

    @Test
    public void testHandleDDLRecord() throws Exception {
        Schema tableSchema = Schema.recordOf(
                Schemas.SCHEMA_RECORD,
                Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                Schema.Field.of("value", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
        );
        StructuredRecord ddlRecord = StructuredRecord.builder(Schemas.DDL_SCHEMA)
                .set(Schemas.TABLE_FIELD, Joiner.on(".").join("demo", dbTableName))
                .set(Schemas.SCHEMA_FIELD, tableSchema.toString())
                .build();

        List<StructuredRecord> input = Stream.of(ddlRecord)
                .map(Schemas::toCDCRecord)
                .collect(Collectors.toList());

        ETLPlugin sourceConfig = MockSource.getPlugin(Schemas.CHANGE_SCHEMA, input);
        sinkConfig = MockSink.getPlugin(TABLENAME);

        programManager = deployETL(sourceConfig, sinkConfig, APP_NAME);
        programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

        AWAIT_PROGRAM.untilAsserted(() -> {
            Table table = bigQuery.getTable(TableId.of(DATASET, TABLENAME));
            Assertions.assertThat(table.getTableId().getTable().equals(TABLENAME))
                    .as("Table '%s' exists", TABLENAME)
                    .isFalse();
        });
    }


    public static class Schemas {
        private static final Schema SIMPLE_TYPES = Schema.unionOf(Arrays.stream(Schema.Type.values())
                .filter(Schema.Type::isSimpleType)
                .map(Schema::of)
                .collect(Collectors.toList()));
        public static final String SCHEMA_RECORD = "schema";
        public static final String TABLE_FIELD = "table";
        public static final String SCHEMA_FIELD = "schema";
        public static final String OP_TYPE_FIELD = "op_type";
        public static final String PRIMARY_KEYS_FIELD = "primary_keys";
        public static final String DDL_FIELD = "ddl";
        public static final String DML_FIELD = "dml";
        public static final String UPDATE_SCHEMA_FIELD = "rows_schema";
        public static final String UPDATE_VALUES_FIELD = "rows_values";
        public static final String CHANGE_TRACKING_VERSION = "change_tracking_version";

        public static final Schema DDL_SCHEMA = Schema.recordOf(
                "DDLRecord",
                Schema.Field.of(TABLE_FIELD, Schema.of(Schema.Type.STRING)),
                Schema.Field.of(SCHEMA_FIELD, Schema.of(Schema.Type.STRING))
        );

        public static final Schema DML_SCHEMA = Schema.recordOf(
                "DMLRecord",
                Schema.Field.of(OP_TYPE_FIELD, enumWith(OperationType.class)),
                Schema.Field.of(TABLE_FIELD, Schema.of(Schema.Type.STRING)),
                Schema.Field.of(PRIMARY_KEYS_FIELD, Schema.arrayOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of(UPDATE_SCHEMA_FIELD, Schema.of(Schema.Type.STRING)),
                Schema.Field.of(UPDATE_VALUES_FIELD, Schema.mapOf(Schema.of(Schema.Type.STRING), SIMPLE_TYPES)),
                Schema.Field.of(CHANGE_TRACKING_VERSION, Schema.of(Schema.Type.STRING))
        );

        public static final Schema CHANGE_SCHEMA = Schema.recordOf(
                "changeRecord",
                Schema.Field.of(DDL_FIELD, Schema.nullableOf(DDL_SCHEMA)),
                Schema.Field.of(DML_FIELD, Schema.nullableOf(DML_SCHEMA))
        );

        public static StructuredRecord toCDCRecord(StructuredRecord changeRecord) {
            String recordName = changeRecord.getSchema().getRecordName();
            if (Objects.equals(recordName, DDL_SCHEMA.getRecordName())) {
                return StructuredRecord.builder(CHANGE_SCHEMA)
                        .set(DDL_FIELD, changeRecord)
                        .build();
            } else if (Objects.equals(recordName, DML_SCHEMA.getRecordName())) {
                return StructuredRecord.builder(CHANGE_SCHEMA)
                        .set(DML_FIELD, changeRecord)
                        .build();
            }
            throw new IllegalArgumentException(String.format("Wrong schema name '%s' for record", recordName));
        }

        public String getTableName(String namespacedTableName) {
            return namespacedTableName.split("\\.")[1];
        }

        private static Schema enumWith(Class<? extends Enum<?>> enumClass) {
            // this method may be removed when Schema.enumWith() method signature fixed
            Enum<?>[] enumConstants = enumClass.getEnumConstants();
            String[] names = new String[enumConstants.length];
            for (int i = 0; i < enumConstants.length; i++) {
                names[i] = enumConstants[i].name();
            }
            return Schema.enumWith(names);
        }

        private Schemas() {
            // utility class
        }
    }

    public enum OperationType {
        INSERT, UPDATE, DELETE;

        public static OperationType fromShortName(String name) {
            switch (name.toUpperCase()) {
                case "I":
                    return INSERT;
                case "U":
                    return UPDATE;
                case "D":
                    return DELETE;
                default:
                    throw new IllegalArgumentException(String.format("Unknown change operation '%s'", name));
            }
        }
    }
}
