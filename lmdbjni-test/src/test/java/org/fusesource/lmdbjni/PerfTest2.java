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
    static Cursor cursor;
    static {
        Transaction tx = env.createTransaction();
        cursor = database.openCursor(tx);
    }

    public static DirectBuffer key = new DirectBuffer(0, 0);
    public static DirectBuffer value = new DirectBuffer(0, 0);

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

    public static int rc = JNI.MDB_NOTFOUND;

    @GenerateMicroBenchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void mdb_cursor_get_address() throws IOException {
        if (rc == JNI.MDB_NOTFOUND) {
            rc = cursor.position(key, value, GetOp.FIRST);
            // de-serialize key/value to make the test more realistic
            key.getLong(0, ByteOrder.BIG_ENDIAN);
            value.getLong(0);
            // rc = JNI.mdb_cursor_get_address(cursor.pointer(), address, address + 2 * Unsafe.ADDRESS_SIZE, JNI.MDB_FIRST);
        } else {
            Util.checkErrorCode(rc);
            rc = cursor.position(key, value, GetOp.NEXT);
            // de-serialize key/value to make the test more realistic
            key.getLong(0, ByteOrder.BIG_ENDIAN);
            value.getLong(0);
            //rc = JNI.mdb_cursor_get_address(cursor.pointer(), address, address + 2 * Unsafe.ADDRESS_SIZE, JNI.MDB_NEXT);
        }
    }
}
