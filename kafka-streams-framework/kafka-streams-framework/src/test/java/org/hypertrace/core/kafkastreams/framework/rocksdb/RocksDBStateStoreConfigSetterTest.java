package org.hypertrace.core.kafkastreams.framework.rocksdb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.junit.jupiter.api.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;

class RocksDBStateStoreConfigSetterTest {

  @Test
  public void testSetConfigBlockSize() {
    String storeName = "test-store";
    Options options = new Options();
    Map<String, Object> configs = new HashMap<>();
    configs.put("rocksdb.block.size", 8388608L);
    RocksDBConfigSetter configSetter = new RocksDBStateStoreConfigSetter();
    configSetter.setConfig(storeName, options, configs);
    assertEquals(((BlockBasedTableConfig) options.tableFormatConfig()).blockSize(), 8388608L);
    configSetter.close(storeName, options);
  }

  @Test
  public void testSetConfigBlockCacheSize() {
    String storeName = "test-store";
    Options options = new Options();
    Map<String, Object> configs = new HashMap<>();
    configs.put("rocksdb.block.cache.size", 33554432L);
    RocksDBConfigSetter configSetter = new RocksDBStateStoreConfigSetter();
    configSetter.setConfig(storeName, options, configs);
    assertEquals(((BlockBasedTableConfig) options.tableFormatConfig()).blockCacheSize(), 33554432L);
  }

  @Test
  public void testSetConfigWriteBufferSize() {
    String storeName = "test-store";
    Options options = new Options();
    Map<String, Object> configs = new HashMap<>();
    configs.put("rocksdb.write.buffer.size", 8388608L);
    RocksDBConfigSetter configSetter = new RocksDBStateStoreConfigSetter();
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.writeBufferSize(), 8388608L);
  }

  @Test
  public void testSetConfigCompressionType() {
    String storeName = "test-store";
    Options options = new Options();
    Map<String, Object> configs = new HashMap<>();
    configs.put("rocksdb.compression.type", "SNAPPY_COMPRESSION");
    RocksDBConfigSetter configSetter = new RocksDBStateStoreConfigSetter();
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.compressionType(), CompressionType.SNAPPY_COMPRESSION);
  }

  @Test
  public void testSetConfigCompactionStyle() {
    String storeName = "test-store";
    Options options = new Options();
    Map<String, Object> configs = new HashMap<>();
    configs.put("rocksdb.compaction.style", "UNIVERSAL");
    RocksDBConfigSetter configSetter = new RocksDBStateStoreConfigSetter();
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.compactionStyle(), CompactionStyle.UNIVERSAL);
  }

  @Test
  public void testSetConfigLogLevel() {
    String storeName = "test-store";
    Options options = new Options();
    Map<String, Object> configs = new HashMap<>();
    configs.put("rocksdb.log.level", "INFO_LEVEL");
    RocksDBConfigSetter configSetter = new RocksDBStateStoreConfigSetter();
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.infoLogLevel(), InfoLogLevel.INFO_LEVEL);
  }

  @Test
  public void testSetConfigMaxWriteBuffer() {
    String storeName = "test-store";
    Options options = new Options();
    Map<String, Object> configs = new HashMap<>();
    configs.put("rocksdb.max.write.buffers", 2);
    RocksDBConfigSetter configSetter = new RocksDBStateStoreConfigSetter();
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.maxWriteBufferNumber(), 2);
  }

  @Test
  public void testSetConfigCacheIndexAndFilterBlocks() {
    String storeName = "test-store";
    Options options = new Options();
    Map<String, Object> configs = new HashMap<>();
    configs.put("rocksdb.cache.index.and.filter.blocks", true);
    RocksDBConfigSetter configSetter = new RocksDBStateStoreConfigSetter();
    configSetter.setConfig(storeName, options, configs);
    assertEquals(((BlockBasedTableConfig) options.tableFormatConfig()).cacheIndexAndFilterBlocks(),
        true);
  }

  @Test
  public void testSetConfigUseDirectReads() {
    String storeName = "test-store";
    Options options = new Options();
    Map<String, Object> configs = new HashMap<>();
    configs.put("rocksdb.direct.reads.enabled", true);
    RocksDBConfigSetter configSetter = new RocksDBStateStoreConfigSetter();
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.useDirectReads(), true);
  }

}