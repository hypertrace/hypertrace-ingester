package org.hypertrace.core.kafkastreams.framework.rocksdb;

import java.util.Map;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Filter;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;

public class RocksDBStateStoreConfigSetter implements RocksDBConfigSetter {

  private static final String ROCKS_DB_PREFIX = "rocksdb.";
  private static final String LOG_LEVEL_CONFIG = ROCKS_DB_PREFIX + "log.level";
  private static final String BLOCK_CACHE_SIZE = ROCKS_DB_PREFIX + "block.cache.size";
  private static final String BLOCK_SIZE = ROCKS_DB_PREFIX + "block.size";
  private static final String WRITE_BUFFER_SIZE = ROCKS_DB_PREFIX + "write.buffer.size";
  private static final String COMPRESSION_TYPE = ROCKS_DB_PREFIX + "compression.type";
  private static final String COMPACTION_STYLE = ROCKS_DB_PREFIX + "compaction.style";
  private static final String MAX_WRITE_BUFFERS = ROCKS_DB_PREFIX + "max.write.buffers";
  private static final String CACHE_INDEX_AND_FILTER_BLOCKS =
      ROCKS_DB_PREFIX + "cache.index.and.filter.blocks";
  private static final String DIRECT_READS_ENABLED = ROCKS_DB_PREFIX + "direct.reads.enabled";
  private static final String OPTIMIZE_FOR_POINT_LOOKUPS =
      ROCKS_DB_PREFIX + "optimize.point.lookups";

  private Filter filter;

  @Override
  public void setConfig(String storeName, Options options, Map<String, Object> configs) {
    BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
    if (tableConfig == null) {
      tableConfig = new BlockBasedTableConfig();
      filter = new BloomFilter();
      tableConfig.setFilter(filter);
    }

    if (configs.containsKey(BLOCK_SIZE)) {
      tableConfig.setBlockSize(Long.valueOf(String.valueOf(configs.get(BLOCK_SIZE))));
    }
    if (configs.containsKey(CACHE_INDEX_AND_FILTER_BLOCKS)) {
      tableConfig.setCacheIndexAndFilterBlocks(
          Boolean.valueOf(String.valueOf(configs.get(CACHE_INDEX_AND_FILTER_BLOCKS))));
    }

    if (configs.containsKey(BLOCK_CACHE_SIZE)) {
      tableConfig.setBlockCacheSize(Long.valueOf(String.valueOf(configs.get(BLOCK_CACHE_SIZE))));
    }

    options.setTableFormatConfig(tableConfig);

    if (configs.containsKey(MAX_WRITE_BUFFERS)) {
      options
          .setMaxWriteBufferNumber(Integer.valueOf(String.valueOf(configs.get(MAX_WRITE_BUFFERS))));
    }

    if (configs.containsKey(WRITE_BUFFER_SIZE)) {
      options.setWriteBufferSize(Long.valueOf(String.valueOf(configs.get(WRITE_BUFFER_SIZE))));
    }

    if (configs.containsKey(COMPACTION_STYLE)) {
      options.setCompactionStyle(CompactionStyle.valueOf((String) configs.get(COMPACTION_STYLE)));
    }

    if (configs.containsKey(COMPRESSION_TYPE)) {
      options.setCompressionType(
          CompressionType.valueOf(String.valueOf(configs.get(COMPRESSION_TYPE))));
    }

    if (configs.containsKey(LOG_LEVEL_CONFIG)) {
      options.setInfoLogLevel(InfoLogLevel.valueOf((String) configs.get(LOG_LEVEL_CONFIG)));
    }

    if (configs.containsKey(DIRECT_READS_ENABLED)) {
      options.setUseDirectReads(Boolean.valueOf(String.valueOf(configs.get(DIRECT_READS_ENABLED))));
    }

    if (configs.containsKey(OPTIMIZE_FOR_POINT_LOOKUPS)) {
      Boolean optimizeForPointLookups = Boolean
          .valueOf(String.valueOf(configs.get(OPTIMIZE_FOR_POINT_LOOKUPS)));
      if (optimizeForPointLookups) {
        long blockCacheSizeMb =
            ((BlockBasedTableConfig) options.tableFormatConfig()).blockCacheSize() / (1024L
                * 1024L);
        options.optimizeForPointLookup(blockCacheSizeMb);
      }
    }
  }

  @Override
  public void close(String storeName, Options options) {
    if (filter != null) {
      filter.close();
    }
  }
}
