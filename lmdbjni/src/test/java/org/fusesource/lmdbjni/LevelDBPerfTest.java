package org.fusesource.lmdbjni;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.junit.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.output.OutputFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LevelDBPerfTest extends Setup {
  static {
    initLevelDb();
  }

  @Test
  public void test() throws RunnerException {
    Options options = new OptionsBuilder()
      .include(".*" + LevelDBPerfTest.class.getSimpleName() + ".*")
      .warmupIterations(10)
      .measurementIterations(10)
      .forks(1)
      .jvmArgs("-server")
      .jvmClasspath(Maven.classPath)
      .outputFormat(OutputFormatType.TextReport)
      .build();
    new Runner(options).run();
  }

  static DBIterator iterator;

  @GenerateMicroBenchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void leveldb_iterate() throws IOException {
    if (iterator == null) {
      iterator = leveldb.iterator();
    }
    if (iterator.hasNext()) {
      Map.Entry<byte[], byte[]> next = iterator.next();
    } else {
      iterator.seekToFirst();
    }
  }
}
