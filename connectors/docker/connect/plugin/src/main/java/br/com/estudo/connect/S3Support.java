package br.com.estudo.connect;

import java.net.URI;
import java.util.*;
import org.apache.iceberg.aws.s3.S3FileIO;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

final class S3Support {
  static Map<String,String> properties(Map<String,String> config) {
    Map<String,String> props = new HashMap<>();
    for(String key:List.of("s3.endpoint","s3.access-key-id","s3.secret-access-key"))
      if(config.containsKey(key) && !config.get(key).isBlank()) props.put(key,config.get(key));
    props.put("s3.path-style-access","true");
    props.put("client.region",config.getOrDefault("s3.region","us-east-1"));
    return props;
  }
  static S3FileIO fileIO(Map<String,String> config) {
    S3FileIO io = new S3FileIO(); io.initialize(properties(config)); return io;
  }
  static void ensureBucket(Map<String,String> config) {
    var builder = S3Client.builder().region(Region.of(config.getOrDefault("s3.region","us-east-1")))
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(config.get("s3.access-key-id"),config.get("s3.secret-access-key"))))
      .forcePathStyle(true);
    if(config.containsKey("s3.endpoint") && !config.get("s3.endpoint").isBlank()) builder.endpointOverride(URI.create(config.get("s3.endpoint")));
    try(S3Client client=builder.build()) {
      String bucket=config.get("s3.bucket");
      try { client.headBucket(r -> r.bucket(bucket)); }
      catch(S3Exception e) {
        if(e.statusCode()!=404) throw e;
        client.createBucket(r -> {
          r.bucket(bucket);
          String region=config.getOrDefault("s3.region","us-east-1");
          if(!region.equals("us-east-1")) r.createBucketConfiguration(c -> c.locationConstraint(region));
        });
      }
    }
  }
}
