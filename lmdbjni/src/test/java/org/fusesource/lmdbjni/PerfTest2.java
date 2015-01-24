package org.fusesource.lmdbjni;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

public class PerfTest2 extends Setup {

  @Test
  public void test() throws RunnerException {
    Options options = new OptionsBuilder()
      .include(".*" + PerfTest2.class.getSimpleName() + ".*")
      .warmupIterations(10)
      .measurementIterations(10)
      .forks(1)
      .jvmArgs("-server")
      .jvmClasspath(Maven.classPath)
      .outputFormat(OutputFormatType.TextReport)
      .build();
    new Runner(options).run();
  }

  static {
    initLMDB();
  }

  public static boolean found = false;
  static BufferCursor cursor;

  public static DirectBuffer key = new DirectBuffer();
  public static DirectBuffer value = new DirectBuffer();

  @GenerateMicroBenchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void mdb_cursor_get_address() throws IOException {
    if (cursor == null) {
      cursor = database.bufferCursor(key, value);
    }
    if (!found) {
      found = cursor.first();
      key.getLong(0);
      value.getLong(0);
    } else {
      found = cursor.next();
      key.getLong(0);
      value.getLong(0);
    }
  }
}
