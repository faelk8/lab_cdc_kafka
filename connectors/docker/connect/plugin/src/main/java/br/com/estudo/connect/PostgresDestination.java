package br.com.estudo.connect;

import java.math.BigDecimal;
import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;
import com.fasterxml.jackson.core.type.TypeReference;

final class PostgresDestination implements Destination {
  private final Connection db;
  PostgresDestination(Map<String,String> props) throws Exception {
    db=DriverManager.getConnection(props.get("connection.url"),props.get("connection.user"),props.get("connection.password"));
    db.setAutoCommit(false);
    try(Statement s=db.createStatement()) {
      s.execute("CREATE SCHEMA IF NOT EXISTS cdc");
      s.execute("CREATE TABLE IF NOT EXISTS cdc.inbox (topic TEXT NOT NULL,partition_id INTEGER NOT NULL,offset_id BIGINT NOT NULL,event JSONB NOT NULL,applied BOOLEAN NOT NULL DEFAULT false,PRIMARY KEY(topic,partition_id,offset_id))");
      s.execute("CREATE INDEX IF NOT EXISTS inbox_pending ON cdc.inbox(topic,partition_id,offset_id) WHERE NOT applied");
    }
    db.commit();
  }
  public void write(List<CdcEvent> events) throws Exception {
    try(PreparedStatement insert=db.prepareStatement("INSERT INTO cdc.inbox(topic,partition_id,offset_id,event) VALUES (?,?,?,?::jsonb) ON CONFLICT DO NOTHING")) {
      for(CdcEvent event:events) {
        insert.setString(1,event.topic);insert.setInt(2,event.partition);insert.setLong(3,event.offset);insert.setString(4,CdcEvent.json(event.event));insert.addBatch();
      }
      insert.executeBatch();db.commit();
    } catch(Exception e) { db.rollback();throw e; }
    idle();
  }
  record Pending(String topic,int partition,long offset,Map<String,Object> event) {}
  public void idle() throws Exception {
    List<Pending> pending=new ArrayList<>();
    try(Statement s=db.createStatement();ResultSet rs=s.executeQuery("SELECT topic,partition_id,offset_id,event FROM (SELECT *,row_number() OVER (PARTITION BY topic ORDER BY partition_id,offset_id) n FROM cdc.inbox WHERE NOT applied AND topic IN ('cliente','produto','pedido','item_pedido','pagamento')) p WHERE n<=500 ORDER BY CASE topic WHEN 'cliente' THEN 1 WHEN 'produto' THEN 2 WHEN 'pedido' THEN 3 WHEN 'item_pedido' THEN 4 ELSE 5 END,partition_id,offset_id")) {
      while(rs.next()) pending.add(new Pending(rs.getString(1),rs.getInt(2),rs.getLong(3),CdcEvent.JSON.readValue(rs.getString(4),new TypeReference<Map<String,Object>>(){})));
    }
    Set<String> blocked=new HashSet<>();
    try {
      for(Pending item:pending) {
        boolean deleted="d".equals(item.event.get("op"));
        Map<String,Object> row=CdcEvent.map(item.event.get(deleted?"before":"after"));
        String identity=item.topic+":"+row.get("id");
        if(blocked.contains(identity)) continue;
        Savepoint savepoint=db.setSavepoint();
        try {
          apply(item.topic,row,deleted);
          try(PreparedStatement mark=db.prepareStatement("UPDATE cdc.inbox SET applied=true WHERE topic=? AND partition_id=? AND offset_id=?")) {
            mark.setString(1,item.topic);mark.setInt(2,item.partition);mark.setLong(3,item.offset);mark.executeUpdate();
          }
          db.releaseSavepoint(savepoint);
        } catch(SQLException e) {
          db.rollback(savepoint);db.releaseSavepoint(savepoint);
          if(!"23503".equals(e.getSQLState())) throw e;
          blocked.add(identity);
        }
      }
      db.commit();
    } catch(Exception e) { db.rollback();throw e; }
  }
  private void apply(String table,Map<String,Object> row,boolean deleted) throws Exception {
    if(!CdcEvent.TABLES.contains(table)) throw new IllegalArgumentException("Unknown table");
    if(deleted) {
      try(PreparedStatement s=db.prepareStatement("DELETE FROM \""+table+"\" WHERE id=?")) { s.setLong(1,((Number)row.get("id")).longValue());s.executeUpdate(); }
      return;
    }
    List<org.apache.iceberg.types.Types.NestedField> fields=CdcEvent.SCHEMAS.get(table).columns().stream().filter(f -> !f.name().startsWith("_")).toList();
    String columns=String.join(",",fields.stream().map(f -> "\""+f.name()+"\"").toList());
    String values=String.join(",",Collections.nCopies(fields.size(),"?"));
    String updates=String.join(",",fields.stream().filter(f -> !f.name().equals("id")).map(f -> "\""+f.name()+"\"=EXCLUDED.\""+f.name()+"\"").toList());
    try(PreparedStatement s=db.prepareStatement("INSERT INTO \""+table+"\" ("+columns+") VALUES ("+values+") ON CONFLICT(id) DO UPDATE SET "+updates)) {
      int index=1;
      for(var field:fields) {
        Object value=row.get(field.name());
        if(value!=null) value=switch(field.type().typeId()) {
          case LONG -> ((Number)value).longValue();case INTEGER -> ((Number)value).intValue();
          case DECIMAL -> new BigDecimal(value.toString());case TIMESTAMP -> OffsetDateTime.parse(value.toString());default -> value;
        };
        s.setObject(index++,value);
      }
      s.executeUpdate();
    }
  }
  public void close() throws Exception { db.close(); }
}
