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
import java.util.concurrent.TimeUnit;

public class PerfTest1 extends Setup {
    @Test
    public void test() throws RunnerException {
        Options options = new OptionsBuilder()
                .include(".*" + PerfTest1.class.getSimpleName() + ".*")
                .warmupIterations(10)
                .measurementIterations(10)
                .forks(1)
                .jvmArgs("-server")
                .jvmClasspath(Maven.classPath)
                .outputFormat(OutputFormatType.TextReport)
                .build();
        new Runner(options).run();
    }
    static Cursor cursor;

    static {
        Transaction tx = env.createTransaction();
        cursor = database.openCursor(tx);
    }

    public static int rc = JNI.MDB_NOTFOUND;


    @GenerateMicroBenchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void mdb_cursor_get_with_deserialization() throws IOException {
        if (rc == JNI.MDB_NOTFOUND) {
            Entry entry = cursor.get(GetOp.FIRST);
            // de-serialize key/value to make the test more realistic
            Bytes.getLong(entry.getKey(), 0);
            Bytes.getLong(entry.getValue(), 0);
        } else {
            Util.checkErrorCode(rc);
            Entry entry = cursor.get(GetOp.NEXT);
            // de-serialize key/value to make the test more realistic
            Bytes.getLong(entry.getKey(), 0);
            Bytes.getLong(entry.getValue(), 0);
        }
    }
/*
    @GenerateMicroBenchmark
    public void mdb_cursor_get_without_deserialization() throws IOException {
        if (rc == JNI.MDB_NOTFOUND) {
            cursor.get(GetOp.FIRST);
        } else {
            Util.checkErrorCode(rc);
            cursor.get(GetOp.NEXT);
        }
    }

    @GenerateMicroBenchmark
    public void mdb_cursor_get() throws IOException {
        if (rc == JNI.MDB_NOTFOUND) {
            rc = JNI.mdb_cursor_get(cursor.pointer(), Setup.keyVal, Setup.valueVal, JNI.MDB_FIRST);
        } else {
            Util.checkErrorCode(rc);
            rc = JNI.mdb_cursor_get(cursor.pointer(), Setup.keyVal, Setup.valueVal, JNI.MDB_NEXT);
        }
    }
*/
}
