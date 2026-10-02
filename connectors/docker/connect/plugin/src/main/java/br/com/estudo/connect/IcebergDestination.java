package br.com.estudo.connect;

import java.time.*;
import java.util.*;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.*;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;

final class IcebergDestination implements Destination {
  private final JdbcCatalog catalog=new JdbcCatalog();
  private final String namespace;
  IcebergDestination(Map<String,String> props) {
    S3Support.ensureBucket(props);
    Map<String,String> settings=S3Support.properties(props);
    settings.put("uri",props.get("catalog.uri"));
    settings.put("jdbc.user",props.get("catalog.user")); settings.put("jdbc.password",props.get("catalog.password"));
    settings.put("warehouse",props.get("catalog.warehouse")); settings.put("io-impl","org.apache.iceberg.aws.s3.S3FileIO");
    catalog.initialize(props.getOrDefault("catalog.name","loja-connect"),settings);
    namespace=props.getOrDefault("catalog.namespace","cdc");
    if(!catalog.namespaceExists(Namespace.of(namespace))) catalog.createNamespace(Namespace.of(namespace));
  }
  public void write(List<CdcEvent> events) throws Exception {
    Map<String,List<CdcEvent>> groups=new LinkedHashMap<>();
    for(CdcEvent event:events) groups.computeIfAbsent(event.table,k -> new ArrayList<>()).add(event);
    for(var group:groups.entrySet()) {
      String name=group.getKey(); var identifier=TableIdentifier.of(namespace,name);
      if(!catalog.tableExists(identifier)) {
        var schema=CdcEvent.SCHEMAS.get(name);
        catalog.createTable(identifier,schema,PartitionSpec.builderFor(schema).day("_event_time").build(),Map.of("format-version","2","write.parquet.compression-codec","zstd"));
      }
      Table table=catalog.loadTable(identifier);
      Map<String,Long> offsets=CdcEvent.JSON.readValue(table.properties().getOrDefault("cdc.offsets","{}"),new TypeReference<Map<String,Long>>(){});
      Map<LocalDate,List<CdcEvent>> days=new LinkedHashMap<>();
      for(CdcEvent event:group.getValue()) {
        if(event.offset<=offsets.getOrDefault(event.progressKey(),-1L)) continue;
        offsets.put(event.progressKey(),event.offset);
        LocalDate day=((OffsetDateTime)event.row.getField("_event_time")).toLocalDate();
        days.computeIfAbsent(day,k -> new ArrayList<>()).add(event);
      }
      if(days.isEmpty()) continue;
      Transaction transaction=table.newTransaction();
      AppendFiles append=transaction.newAppend();
      for(var day:days.entrySet()) {
        PartitionData partition=new PartitionData(table.spec().partitionType());
        partition.set(0,(int)day.getKey().toEpochDay());
        String location=table.location()+"/data/_event_time_day="+day.getKey()+"/"+UUID.randomUUID()+".parquet";
        DataWriter<Record> writer=Parquet.writeData(table.io().newOutputFile(location)).forTable(table)
          .createWriterFunc(GenericParquetWriter::buildWriter).withSpec(table.spec()).withPartition(partition).build();
        try(writer) { for(CdcEvent event:day.getValue()) writer.write(event.row); }
        append.appendFile(writer.toDataFile());
      }
      append.commit();
      transaction.updateProperties().set("cdc.offsets",CdcEvent.json(offsets)).commit();
      transaction.commitTransaction();
    }
  }
  public void close() throws Exception { catalog.close(); }
}
