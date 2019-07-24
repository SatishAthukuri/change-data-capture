package io.cdap.plugin.cdc.integration;

import com.codahale.metrics.MetricRegistry;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.datastreams.DataStreamsApp;
import io.cdap.cdap.datastreams.DataStreamsSparkLauncher;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.plugin.cdc.sink.BigQueryStreamingSink;
import io.cdap.plugin.cdc.util.BigQueryUtil;
import io.cdap.plugin.cdc.util.GCPConfig;
import kafka.serializer.DefaultDecoder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BigQueryStreamingSinkTestBase extends HydratorTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryStreamingSinkTestBase.class);
    private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-streams", "1.0.0");
    private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-streams", "1.0.0");

    @ClassRule
    public static final TestConfiguration CONFIG =
            new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                    Constants.AppFabric.SPARK_COMPAT, Compat.SPARK_COMPAT);

    @BeforeClass
    public static void setupTest() throws Exception {
        LOG.info("Setting up application");

        setupStreamingArtifacts(APP_ARTIFACT_ID, DataStreamsApp.class);

        LOG.info("Setting up plugins");

        addPluginArtifact(NamespaceId.DEFAULT.artifact("BigQueryStreaming", "1.0.0"),
                APP_ARTIFACT_ID,
                BigQueryStreamingSink.class,
                MetricRegistry.class,
                BigQueryUtil.class, GCPConfig.class, DefaultDecoder.class);
    }

    protected SparkManager deployETL(ETLPlugin sourcePlugin, ETLPlugin sinkPlugin, String appName) throws Exception {
        ETLStage source = new ETLStage("source", sourcePlugin);
        ETLStage sink = new ETLStage("sink", sinkPlugin);
        DataStreamsConfig etlConfig = DataStreamsConfig.builder()
                .addStage(source)
                .addStage(sink)
                .addConnection(source.getName(), sink.getName())
                .setBatchInterval("1s")
                .build();

        AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
        ApplicationId appId = NamespaceId.DEFAULT.app(appName);
        ApplicationManager applicationManager = deployApplication(appId, appRequest);
        return getProgramManager(applicationManager);
    }

    private SparkManager getProgramManager(ApplicationManager appManager) {
        return appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    }
}
