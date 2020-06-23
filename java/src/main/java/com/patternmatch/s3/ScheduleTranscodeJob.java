package com.patternmatch.s3;

import com.amazonaws.services.elastictranscoder.AmazonElasticTranscoder;
import com.amazonaws.services.elastictranscoder.AmazonElasticTranscoderClient;
import com.amazonaws.services.elastictranscoder.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.log4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

public class ScheduleTranscodeJob implements RequestHandler<S3Event, Void> {

    private static final Logger LOG = Logger.getLogger(ScheduleTranscodeJob.class);

    private static final String HLS_0400K_PRESET_ID = "1351620000001-200050";

    private static final String SEGMENT_DURATION = "2";

    @Override
    public Void handleRequest(S3Event event, Context context) {
        try {
            final String elasticTranscoderPipelineId = System.getenv("ELASTIC_TRANSCODER_PIPELINE_ID");
            final String transcodedOutputKeyPrefix = System.getenv("TRANSCODED_VIDEOS_PREFIX");

            LOG.info("[LAMBDA] ENV - Elastic Transcoder Pipeline ID: " + elasticTranscoderPipelineId);
            LOG.info("[LAMBDA] ENV - Transcoded Video Prefix: " + transcodedOutputKeyPrefix);

            LOG.info("[S3] Received an event: " + event);

            final AmazonElasticTranscoder elasticTranscoderClient = AmazonElasticTranscoderClient.builder().build();
            final AmazonS3 s3Client = AmazonS3Client.builder().build();

            for (S3EventNotification.S3EventNotificationRecord record : event.getRecords()) {
                String s3FullKey = record.getS3().getObject().getKey();

                LOG.info("[S3] Processing key: " + s3FullKey);

                Path path = Paths.get(s3FullKey);
                String s3Key = path.getFileName().toString();

                LOG.info("[S3] Processing file: " + s3Key);

                CreateJobOutput hls0400k =
                    new CreateJobOutput()
                        .withKey("hls0400k/" + s3Key)
                        .withPresetId(HLS_0400K_PRESET_ID)
                        .withSegmentDuration(SEGMENT_DURATION)
                        .withThumbnailPattern("thumbnails/{count}-" + s3Key);

                List<CreateJobOutput> outputs = Collections.singletonList(hls0400k);

                CreateJobPlaylist playlist =
                    new CreateJobPlaylist()
                        .withName("hls_playlist_" + s3Key)
                        .withFormat("HLSv3")
                        .withOutputKeys(hls0400k.getKey());

                JobInput input = new JobInput().withKey(s3FullKey);

                CreateJobRequest createJobRequest = new CreateJobRequest()
                    .withPipelineId(elasticTranscoderPipelineId)
                    .withInput(input)
                    .withOutputKeyPrefix(transcodedOutputKeyPrefix + "/" + s3Key + "/")
                    .withOutputs(outputs)
                    .withPlaylists(playlist);

                Job job = elasticTranscoderClient.createJob(createJobRequest).getJob();

                LOG.info("[ELASTIC TRANSCODER] Job created: " + job.getId());

                String s3Bucket = record.getS3().getBucket().getName();
                CopyObjectRequest copyObjectRequest = new CopyObjectRequest(s3Bucket, s3FullKey, s3Bucket, s3FullKey);
                ObjectMetadata metadata = new ObjectMetadata();

                metadata.addUserMetadata("et-job-id", job.getId());
                copyObjectRequest.setNewObjectMetadata(metadata);

                LOG.info("[S3] Object metadata updated successfully: " + String.format("%s/%s", s3Bucket, s3FullKey));
                s3Client.copyObject(copyObjectRequest);
            }
        } catch(Exception error) {
            LOG.info("[LAMBDA] Exception: " + error.toString());
        }

        return null;
    }
}
