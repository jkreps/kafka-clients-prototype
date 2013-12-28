package kafka.clients.producer;

import static org.junit.Assert.*;
import static java.util.Arrays.asList;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import kafka.common.TopicPartition;
import kafka.common.record.CompressionType;
import kafka.common.record.LogEntry;
import kafka.common.record.Record;
import kafka.common.record.Records;
import kafka.common.utils.MockTime;

import org.junit.Test;

public class RecordAccumulatorTest {
  
  private TopicPartition tp = new TopicPartition("test", 0);
  private MockTime time = new MockTime();
  private byte[] key = "key".getBytes();
  private byte[] value = "value".getBytes();
  
  @Test
  public void testFull() throws Exception {
    long now = time.milliseconds();
    RecordAccumulator accum = new RecordAccumulator(1024, 10*1024, 10L, time);
    int msgSize = Records.LOG_OVERHEAD + Record.recordSize(key.length, value.length);
    int appends = 1024 / msgSize;
    for(int i = 0; i < appends; i++) {
      accum.append(tp, key, value, CompressionType.NONE, null);
      assertEquals("No partitions should be ready.", 0, accum.ready(now).size());
    }
    accum.append(tp, key, value, CompressionType.NONE, null);
    assertEquals("Our partition should be ready", asList(tp), accum.ready(time.milliseconds()));
    List<RecordBatch> batches = accum.drain(asList(tp), Integer.MAX_VALUE);
    assertEquals(1, batches.size());
    RecordBatch batch = batches.get(0);
    Iterator<LogEntry> iter = batch.records.iterator();
    for(int i = 0; i < appends; i++) {
      LogEntry entry = iter.next();
      assertEquals("Keys should match", ByteBuffer.wrap(key), entry.record().key());
      assertEquals("Values should match", ByteBuffer.wrap(value), entry.record().value());
    }
    assertFalse("No more records", iter.hasNext());
  }

  @Test
  public void testLinger() throws Exception {
    long lingerMs = 10L;
    RecordAccumulator accum = new RecordAccumulator(1024, 10*1024, lingerMs, time);
    accum.append(tp, key, value, CompressionType.NONE, null);
    assertEquals("No partitions should be ready", 0, accum.ready(time.milliseconds()).size());
    time.sleep(10);
    assertEquals("Our partition should be ready", asList(tp), accum.ready(time.milliseconds()));
    List<RecordBatch> batches = accum.drain(asList(tp), Integer.MAX_VALUE);
    assertEquals(1, batches.size());
    RecordBatch batch = batches.get(0);
    Iterator<LogEntry> iter = batch.records.iterator();
    LogEntry entry = iter.next();
    assertEquals("Keys should match", ByteBuffer.wrap(key), entry.record().key());
    assertEquals("Values should match", ByteBuffer.wrap(value), entry.record().value());
    assertFalse("No more records", iter.hasNext());
  }
  
  @Test
  public void testPartialDrain() {
    
  }
    
}
