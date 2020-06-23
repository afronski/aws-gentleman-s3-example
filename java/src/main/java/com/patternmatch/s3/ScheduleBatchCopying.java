package com.patternmatch.s3;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.AWSS3ControlClient;
import com.amazonaws.services.s3control.model.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.amazonaws.util.IOUtils.copy;

public class ScheduleBatchCopying implements RequestHandler<S3Event, Void> {

    private static final Logger LOG = Logger.getLogger(ScheduleBatchCopying.class);

    @Override
    public Void handleRequest(S3Event event, Context context) {
        try {
            final String accountId = System.getenv("ACCOUNT_ID");
            final String manifestPrefix = System.getenv("MANIFEST_PREFIX");
            final String outputBucket = System.getenv("OUTPUT_BUCKET");
            final String iamRoleArn = System.getenv("IAM_ROLE_ARN");

            LOG.info("[LAMBDA] ENV - Account ID: " + accountId);
            LOG.info("[LAMBDA] ENV - Manifest Prefix: " + manifestPrefix);
            LOG.info("[LAMBDA] ENV - Output Bucket: " + outputBucket);
            LOG.info("[LAMBDA] ENV - IAM Role for S3 Batch Operation: " + iamRoleArn);

            LOG.info("[S3] Received an event: " + event);

            final AmazonS3 s3Client = AmazonS3Client.builder().build();

            for (S3EventNotification.S3EventNotificationRecord record : event.getRecords()) {
                String bucket = record.getS3().getBucket().getName();
                String fullKey = record.getS3().getObject().getKey();
                String query = "SELECT s._1, s._2 FROM s3object s";

                SelectObjectContentRequest selectRequest = generateBaseCSVRequest(bucket, fullKey, query);

                Path path = Paths.get(fullKey);
                final String key = path.getFileName().toString();

                final AtomicBoolean isResultComplete = new AtomicBoolean(false);
                final String outputManifestPath = manifestPrefix + "/" + key;
                String payload;

                try (
                    OutputStream outputStream = new ByteArrayOutputStream();
                    SelectObjectContentResult result = s3Client.selectObjectContent(selectRequest)) {
                    InputStream resultInputStream = result.getPayload().getRecordsInputStream(
                        new SelectObjectContentEventVisitor() {
                            @Override
                            public void visit(SelectObjectContentEvent.EndEvent event) {
                                isResultComplete.set(true);
                                LOG.info("[S3] Received S3 Select result: " + event);
                            }
                        }
                    );

                    copy(resultInputStream, outputStream);
                    payload = outputStream.toString();
                }

                LOG.info("[S3] Manifest generated: " + outputManifestPath);
                PutObjectResult manifestPutResult = s3Client.putObject(bucket, outputManifestPath, payload);

                if (!isResultComplete.get()) {
                    throw new Exception("S3 Select request was incomplete as End Event was not received.");
                }

                JobOperation jobOperation =
                    new JobOperation()
                        .withS3PutObjectCopy(
                            new S3CopyObjectOperation()
                                .withTargetResource("arn:aws:s3:::" + outputBucket)
                                .withMetadataDirective(S3MetadataDirective.COPY)
                                .withStorageClass(S3StorageClass.STANDARD)
                        );

                JobManifest manifest =
                    new JobManifest()
                        .withSpec(
                            new JobManifestSpec()
                                .withFormat("S3BatchOperations_CSV_20180820")
                                .withFields("Bucket", "Key"))
                        .withLocation(
                            new JobManifestLocation()
                                .withObjectArn("arn:aws:s3:::" + bucket + "/" + outputManifestPath)
                                .withETag(manifestPutResult.getETag()));

                JobReport jobReport =
                    new JobReport()
                        .withBucket("arn:aws:s3:::" + bucket)
                        .withPrefix("reports")
                        .withFormat("Report_CSV_20180820")
                        .withEnabled(true)
                        .withReportScope("AllTasks");

                AWSS3Control s3ControlClient = AWSS3ControlClient.builder().build();

                String uuid = UUID.randomUUID().toString();
                s3ControlClient.createJob(new CreateJobRequest()
                    .withAccountId(accountId)
                    .withOperation(jobOperation)
                    .withManifest(manifest)
                    .withReport(jobReport)
                    .withPriority(10)
                    .withRoleArn(iamRoleArn)
                    .withClientRequestToken(uuid)
                    .withDescription("Copying final transcoded videos for " + key)
                    .withConfirmationRequired(false)
                );
            }
        } catch(Exception error) {
            LOG.info("[LAMBDA] Exception: " + error.toString());
        }

        return null;
    }

    private static SelectObjectContentRequest generateBaseCSVRequest(String bucket, String key, String query) {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(bucket);
        request.setKey(key);
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);

        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setCsv(new CSVInput());
        inputSerialization.setCompressionType(CompressionType.NONE);
        request.setInputSerialization(inputSerialization);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setCsv(new CSVOutput());
        request.setOutputSerialization(outputSerialization);

        return request;
    }
}
