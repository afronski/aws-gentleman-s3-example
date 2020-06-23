package com.patternmatch.s3;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.patternmatch.s3.model.JobStatusNotification;
import org.apache.log4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class GenerateListOfFilesAfterTranscoding implements RequestHandler<SNSEvent, Void> {

    private static final Logger LOG = Logger.getLogger(GenerateListOfFilesAfterTranscoding.class);

    @Override
    public Void handleRequest(SNSEvent event, Context context) {
        try {
            final String fileListsPrefixOutput = System.getenv("FILE_LISTS_PREFIX");
            final String inputBucket = System.getenv("INPUT_BUCKET");

            LOG.info("[LAMBDA] ENV - File Lists Prefix: " + fileListsPrefixOutput);

            LOG.info("[SNS] Received an event: " + event);

            final AmazonS3 s3Client = AmazonS3Client.builder().build();
            final ObjectMapper mapper = new ObjectMapper();

            for (SNSEvent.SNSRecord record : event.getRecords()) {
                final JobStatusNotification notification =
                    mapper.readValue(record.getSNS().getMessage(), JobStatusNotification.class);

                LOG.info("[ELASTIC TRANSCODER] Job status: " + notification.getState());
                LOG.info("[ELASTIC TRANSCODER] Job ID: " + notification.getJobId());
                LOG.info("[ELASTIC TRANSCODER] Pipeline ID: " + notification.getPipelineId());

                ObjectListing listing = s3Client.listObjects(inputBucket, notification.getOutputKeyPrefix());
                List<S3ObjectSummary> summaries = listing.getObjectSummaries();

                while (listing.isTruncated()) {
                    listing = s3Client.listNextBatchOfObjects(listing);
                    summaries.addAll(listing.getObjectSummaries());
                }

                LOG.info("[S3] Listing: " + inputBucket + "/" + notification.getOutputKeyPrefix());
                LOG.info("[S3] Found following number of files: " + summaries.size());

                List<String> csvLines =
                    summaries
                        .stream()
                        .map(summary -> summary.getBucketName() + "," + summary.getKey() + "," + summary.getSize())
                        .collect(Collectors.toList());

                Path path = Paths.get(notification.getInput().getKey());
                String inputFileName = path.getFileName().toString();

                String csvPayload = String.join("\n", csvLines);
                String finalKey = fileListsPrefixOutput + "/" + inputFileName + ".csv";

                LOG.info("[S3] Saving CSV: " + finalKey);

                s3Client.putObject(inputBucket, finalKey, csvPayload);
            }

        } catch(Exception error) {
            LOG.info("[LAMBDA] Exception: " + error.toString());
        }

        return null;
    }
}
