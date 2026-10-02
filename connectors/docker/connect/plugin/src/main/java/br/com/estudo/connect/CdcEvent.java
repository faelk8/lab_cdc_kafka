package br.com.estudo.connect;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Type;
import org.apache.kafka.connect.errors.DataException;

final class CdcEvent {
  static final ObjectMapper JSON = new ObjectMapper();
  static final List<String> TABLES = List.of("cliente", "produto", "pedido", "item_pedido", "pagamento");
  static final Map<String,Schema> SCHEMAS = new LinkedHashMap<>();
  static {
    schema("cliente", "id:L,nome:S,email:S,cidade:S");
    schema("produto", "id:L,nome:S,preco:D");
    schema("pedido", "id:L,cliente_id:L,criado_em:T,status:S,total:D");
    schema("item_pedido", "id:L,pedido_id:L,produto_id:L,quantidade:I,preco_unitario:D");
    schema("pagamento", "id:L,pedido_id:L,valor:D,metodo:S,status:S");
  }
  static void schema(String table, String columns) {
    List<Types.NestedField> fields = new ArrayList<>();
    int id = 1;
    for (String column : (columns+",_topic:S,_partition:I,_offset:L,_op:S,_event_time:T,_deleted:B,_before:S,_after:S").split(",")) {
      String[] parts = column.split(":");
      Type type = switch (parts[1]) {
        case "L" -> Types.LongType.get(); case "I" -> Types.IntegerType.get();
        case "D" -> Types.DecimalType.of(12,2); case "T" -> Types.TimestampType.withZone();
        case "B" -> Types.BooleanType.get(); default -> Types.StringType.get();
      };
      fields.add(Types.NestedField.optional(id++, parts[0], type));
    }
    SCHEMAS.put(table, new Schema(fields));
  }
  static Object plain(Object value) {
    if (value instanceof Struct struct) {
      Map<String,Object> result = new LinkedHashMap<>();
      for (var field : struct.schema().fields()) result.put(field.name(), plain(struct.get(field)));
      return result;
    }
    if (value instanceof Map<?,?> map) {
      Map<String,Object> result = new LinkedHashMap<>();
      map.forEach((k,v) -> result.put(k.toString(), plain(v)));
      return result;
    }
    if (value instanceof Collection<?> values) return values.stream().map(CdcEvent::plain).toList();
    return value;
  }
  @SuppressWarnings("unchecked")
  static Map<String,Object> map(Object value) { return (Map<String,Object>) value; }
  static String json(Object value) {
    try { return JSON.writeValueAsString(value); }
    catch (Exception e) { throw new DataException("Invalid CDC JSON", e); }
  }
  final String table, topic, op;
  final int partition;
  final long offset;
  final Map<String,Object> event, business;
  final GenericRecord row;
  CdcEvent(SinkRecord record) {
    topic = record.topic(); table = topic;
    if (!TABLES.contains(table)) throw new DataException("Unexpected table topic: "+table);
    partition = record.kafkaPartition(); offset = record.kafkaOffset();
    event = map(plain(record.value())); op = (String) event.get("op");
    if (!Set.of("r","c","u","d").contains(op)) throw new DataException("Unsupported operation: "+op);
    business = map(event.get(op.equals("d") ? "before" : "after"));
    Map<String,Object> source = map(event.get("source"));
    Number timestamp = source == null ? null : (Number) source.get("ts_ms");
    if (timestamp == null || timestamp.longValue() == 0) timestamp = (Number) event.get("ts_ms");
    if (timestamp == null) throw new DataException("CDC timestamp missing");
    row = GenericRecord.create(SCHEMAS.get(table));
    for (var field : SCHEMAS.get(table).columns()) {
      Object value = business.get(field.name());
      if (value != null) value = switch (field.type().typeId()) {
        case LONG -> ((Number)value).longValue(); case INTEGER -> ((Number)value).intValue();
        case DECIMAL -> new BigDecimal(value.toString()).setScale(2);
        case TIMESTAMP -> OffsetDateTime.parse(value.toString()).withOffsetSameInstant(ZoneOffset.UTC);
        default -> value;
      };
      row.setField(field.name(), value);
    }
    row.setField("_topic", topic); row.setField("_partition", partition); row.setField("_offset", offset);
    row.setField("_op", op); row.setField("_event_time", Instant.ofEpochMilli(timestamp.longValue()).atOffset(ZoneOffset.UTC));
    row.setField("_deleted", op.equals("d")); row.setField("_before", json(event.get("before"))); row.setField("_after", json(event.get("after")));
  }
  String progressKey() { return topic+":"+partition; }
  static String identity(org.apache.iceberg.data.Record row) {
    return row.getField("_topic")+":"+row.getField("_partition")+":"+row.getField("_offset");
  }
}
