package br.com.estudo.connect;

import java.util.*;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

public class CdcSinkTask extends SinkTask {
  private Destination destination;
  public String version() { return CdcSinkConnector.VERSION; }
  public void start(Map<String,String> props) {
    try {
      destination = switch(props.get("destination.type")) {
        case "s3" -> new DailyParquetDestination(props);
        case "iceberg" -> new IcebergDestination(props);
        case "postgres" -> new PostgresDestination(props);
        default -> throw new ConnectException("Unknown destination");
      };
    } catch(Exception e) { throw new ConnectException("Could not initialize sink destination", e); }
  }
  public void put(Collection<SinkRecord> records) {
    try {
      List<CdcEvent> events = new ArrayList<>();
      for (SinkRecord record : records) if(record.value()!=null) events.add(new CdcEvent(record));
      destination.write(events);
    } catch(Exception e) { throw new ConnectException("Sink write failed; offsets were not confirmed", e); }
  }
  public void flush(Map<TopicPartition,OffsetAndMetadata> offsets) {
    try { destination.idle(); }
    catch(Exception e) { throw new ConnectException("Sink projection failed",e); }
  }
  public void stop() {
    try { if(destination!=null) destination.close(); }
    catch(Exception e) { throw new ConnectException("Sink close failed",e); }
  }
}
interface Destination extends AutoCloseable {
  void write(List<CdcEvent> events) throws Exception;
  default void idle() throws Exception {}
  default void close() throws Exception {}
}
