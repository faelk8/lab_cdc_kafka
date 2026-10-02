package br.com.estudo.connect;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;

final class DailyParquetDestination implements Destination {
  private final S3FileIO io;
  private final String root;
  private final ZoneId zone;
  DailyParquetDestination(Map<String,String> props) {
    S3Support.ensureBucket(props); io=S3Support.fileIO(props);
    root="s3://"+props.get("s3.bucket")+"/"+props.getOrDefault("s3.prefix","parquet-native").replaceAll("^/+|/+$","");
    zone=ZoneId.of(props.getOrDefault("s3.timezone","America/Sao_Paulo"));
  }
  public void write(List<CdcEvent> events) throws Exception {
    Map<String,List<CdcEvent>> groups=new LinkedHashMap<>();
    for(CdcEvent event:events) {
      var time=(OffsetDateTime)event.row.getField("_event_time");
      String date=time.atZoneSameInstant(zone).format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
      groups.computeIfAbsent(root+"/"+event.table+"/"+date+".parquet", k -> new ArrayList<>()).add(event);
    }
    for(var group:groups.entrySet()) {
      String location=group.getKey(); var schema=CdcEvent.SCHEMAS.get(group.getValue().get(0).table);
      Map<String,Record> rows=new TreeMap<>();
      var input=io.newInputFile(location);
      if(input.exists()) {
        try(var records=Parquet.read(input).project(schema).createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema,fileSchema)).build()) {
          for(Object value:records) { Record row=(Record)value; rows.put(CdcEvent.identity(row),row.copy()); }
        }
      }
      for(CdcEvent event:group.getValue()) rows.put(CdcEvent.identity(event.row),event.row);
      try(FileAppender<Record> writer=Parquet.write(io.newOutputFile(location)).schema(schema)
          .set("write.parquet.compression-codec","zstd").createWriterFunc(GenericParquetWriter::buildWriter).overwrite().build()) {
        for(Record row:rows.values()) writer.add(row);
      }
    }
  }
  public void close() { io.close(); }
}
