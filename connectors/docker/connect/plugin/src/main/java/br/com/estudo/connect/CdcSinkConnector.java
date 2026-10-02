package br.com.estudo.connect;

import java.util.*;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/** Native Kafka Connect sink; no external Python consumer forwards records. */
public class CdcSinkConnector extends SinkConnector {
  private Map<String,String> properties;
  public static final String VERSION = "1.0.0";
  public String version() { return VERSION; }
  public void start(Map<String,String> props) {
    config().parse(props);
    if (!"1".equals(props.getOrDefault("tasks.max", "1")))
      throw new org.apache.kafka.common.config.ConfigException("Use tasks.max=1: one writer per destination");
    properties = new HashMap<>(props);
  }
  public Class<? extends Task> taskClass() { return CdcSinkTask.class; }
  public List<Map<String,String>> taskConfigs(int maxTasks) { return List.of(new HashMap<>(properties)); }
  public void stop() {}
  public ConfigDef config() {
    ConfigDef d = new ConfigDef();
    d.define("destination.type", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.ValidString.in("s3", "iceberg", "postgres"), ConfigDef.Importance.HIGH, "Destination sink type");
    for (String[] field : new String[][] {
      {"s3.endpoint", "http://minio:9000"}, {"s3.bucket", "cdc-loja"}, {"s3.access-key-id", "estudo"},
      {"s3.region", "us-east-1"}, {"s3.prefix", "parquet-native"}, {"s3.timezone", "America/Sao_Paulo"},
      {"catalog.name", "loja-connect"}, {"catalog.uri", "jdbc:postgresql://postgres-destino:5432/loja"},
      {"catalog.user", "estudo"}, {"catalog.namespace", "cdc"}, {"catalog.warehouse", "s3://cdc-loja/iceberg-native"},
      {"connection.url", "jdbc:postgresql://postgres-destino:5432/loja"}, {"connection.user", "estudo"}
    }) d.define(field[0], ConfigDef.Type.STRING, field[1], ConfigDef.Importance.MEDIUM, field[0]);
    for (String key : List.of("s3.secret-access-key", "catalog.password", "connection.password"))
      d.define(key, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.HIGH, key);
    return d;
  }
}
