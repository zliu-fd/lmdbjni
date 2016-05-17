package org.fusesource.lmdbjni;

import baseline.*;
import baseline.CarDecoder.FuelFiguresDecoder;
import baseline.CarDecoder.PerformanceFiguresDecoder;
import baseline.CarDecoder.PerformanceFiguresDecoder.AccelerationDecoder;
import java.io.IOException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.agrona.MutableDirectBuffer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SbeTest {

  // buffers and flyweights
  private static final int MAX_VAL_SIZE = 4096;
  private static final MutableDirectBuffer MDB_KEY = Buffers.buffer(0);
  private static final MutableDirectBuffer MDB_VAL = Buffers.buffer(0);
  private static final MessageHeaderDecoder MESSAGE_HEADER_DECODER = new MessageHeaderDecoder();
  private static final MessageHeaderEncoder MESSAGE_HEADER_ENCODER = new MessageHeaderEncoder();
  private static final CarDecoder CAR_DECODER = new CarDecoder();
  private static final CarEncoder CAR_ENCODER = new CarEncoder();

  // static test data (for consistency across storage and verification steps)
  private static final byte[] VEHICLE_CODE;
  private static final byte[] MANUFACTURER_CODE;
  private static final byte[] MAKE;
  private static final byte[] MODEL;
  private static final int MODEL_YEAR = 2013;
  private static final MutableDirectBuffer ACTIVATION_CODE;
  private static final BooleanType AVAILABLE = BooleanType.T;
  private static final Model CODE = Model.A;
  private static final int CAPACITY = 2000;
  
  // test serial number generation (to ensure Endian byte boundary crossed)
  private static final int SN_BEGIN = 250;
  private static final int SN_UNTIL = 260;

  static {
    try {
      VEHICLE_CODE = "abcdef".getBytes(CarEncoder.vehicleCodeCharacterEncoding());
      MANUFACTURER_CODE = "123".getBytes(EngineEncoder.manufacturerCodeCharacterEncoding());
      MAKE = "Honda".getBytes(CarEncoder.makeCharacterEncoding());
      MODEL = "Civic VTi".getBytes(CarEncoder.modelCharacterEncoding());
      final byte[] code = "abcdef".getBytes(CarEncoder.activationCodeCharacterEncoding());
      ACTIVATION_CODE = Buffers.buffer(code.length);
      ACTIVATION_CODE.putBytes(0, code);
    } catch (final UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void resetBuffers() {
    MDB_KEY.wrap(Buffers.buffer(Long.BYTES).byteBuffer());
    MDB_VAL.wrap(Buffers.buffer(MAX_VAL_SIZE).byteBuffer());
  }
  
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();
  
  @Before
  public void before() {
    resetBuffers();
  }
  
  @Test
  public void verifySbeBufferReuseWithoutAnyLmdbInvolvement() {
    for (int sn = SN_UNTIL; sn >= SN_BEGIN; sn--) {
      encodeSbe(sn);
      decodeSbe(sn);
    }
  }

  @Test(expected = AssertionError.class)
  public void verifySbeDecodeReallyVerifiesTheSerialNumber() {
    encodeSbe(100);
    decodeSbe(99); // wrong SN
  }
  
  /**
   * Exemplifies how to share zero copy buffers between LMDB and SBE.
   * <p>
   * For simplicity we are using a {@link Long} key here. It is feasible to use
   * an SBE-encoded key instead, but it would over-complicate this test (as we
   * would need an extra SBE decoder flyweight and encoder flyweight). If you
   * intend to encode your keys via SBE, carefully consider (a) the byte order
   * and (b) the effect of SBE composites (eg <code>messageHeader</code>,
   * <code>varDataEncoding</code>, <code>groupSizeEncoding</code>) in the keys.
   * These factors are likely to yield keys in a different order than you
   * probably want. If storing SBE-encoded keys you might prefer to declare the
   * SBE schema as Big Endian byte order, and rely on either SBE composites or
   * very simple message types (without any requirement for headers, versions,
   * variable length encoded types etc).
   *
   * @throws IOException
   */
  @Test
  public void verifySbeValuesStoreViaBufferCursor() throws IOException {
    try (Env env = new Env()) {
      env.setMapSize(10, ByteUnit.MEBIBYTES);
      env.open(tmp.newFolder().getCanonicalPath());
      Env.pushMemoryPool(1024);

      try (Database db = env.openDatabase("cars");) {
        for (int sn = SN_UNTIL; sn >= SN_BEGIN; sn--) {
          encodeSbe(sn);
          MDB_KEY.putLong(0, sn, ByteOrder.BIG_ENDIAN);
          try (Transaction tx = env.createWriteTransaction()) {
            try (BufferCursor cursor = db.bufferCursor(tx, MDB_KEY, MDB_VAL)) {
              cursor.setWriteMode();
              boolean result = cursor.overwrite();
              assertThat(result, is(true));
            }
            tx.commit();
          }
        }

        readUsingCursorNext(env, db);
        readUsingCursorPrev(env, db);
      }

      Env.popMemoryPool();
    }
  }
  
  private void readUsingCursorNext(Env env, Database db) {
    resetBuffers();
    try (Transaction tx = env.createReadTransaction();) {
      try (BufferCursor cursor = db.bufferCursor(tx, MDB_KEY, MDB_VAL);) {
        cursor.setWriteMode();
        assertThat(MDB_KEY.byteBuffer(), not(nullValue()));
        long sn = SN_BEGIN;
        MDB_KEY.putLong(0, sn, ByteOrder.BIG_ENDIAN); // legal before seek
        assertThat(cursor.seekKey(), is(true)); // MDB_* now read-only
        assertThat(cursor.keyLength(), not(0));
        assertThat(cursor.valLength(), not(0));
        decodeSbe(sn);

        // iterate remainder
        int done = 1;
        while (cursor.next()) {
          sn = SN_BEGIN + done;
          assertThat(MDB_KEY.getLong(0, ByteOrder.BIG_ENDIAN), is(sn));
          assertThat(cursor.keyLength(), not(0));
          assertThat(cursor.valLength(), not(0));
          decodeSbe(sn);
          done++;
        }
        assertThat(done, is(SN_UNTIL - SN_BEGIN + 1));
        
        // some extra seeks
        sn = SN_BEGIN;
        assertThat(cursor.first(), is(true));
        assertThat(MDB_KEY.getLong(0, ByteOrder.BIG_ENDIAN), is(sn));
        decodeSbe(sn);
        
        sn = SN_UNTIL;
        assertThat(cursor.last(), is(true));
        assertThat(MDB_KEY.getLong(0, ByteOrder.BIG_ENDIAN), is(sn));
        decodeSbe(sn);
      }
    }
  }
  
  private void readUsingCursorPrev(Env env, Database db) {
    resetBuffers();
    try (Transaction tx = env.createReadTransaction();) {
      try (BufferCursor cursor = db.bufferCursor(tx, MDB_KEY, MDB_VAL);) {
        cursor.setWriteMode();
        assertThat(MDB_KEY.byteBuffer(), not(nullValue()));
        long sn = SN_UNTIL;
        MDB_KEY.putLong(0, sn, ByteOrder.BIG_ENDIAN); // legal before seek
        assertThat(cursor.seekKey(), is(true)); // MDB_* now read-only
        assertThat(cursor.keyLength(), not(0));
        assertThat(cursor.valLength(), not(0));
        decodeSbe(sn);

        // iterate remainder
        int done = 1;
        while (cursor.prev()) {
          sn = SN_UNTIL - done;
          assertThat(MDB_KEY.getLong(0, ByteOrder.BIG_ENDIAN), is(sn));
          assertThat(cursor.keyLength(), not(0));
          assertThat(cursor.valLength(), not(0));
          decodeSbe(sn);
          done++;
        }
        assertThat(done, is(SN_UNTIL - SN_BEGIN + 1));
      }
    }
  }
  
  private void encodeSbe(long serialNumber) {
    int bufferOffset = 0;
    MESSAGE_HEADER_ENCODER
        .wrap(MDB_VAL, bufferOffset)
        .blockLength(CAR_ENCODER.sbeBlockLength())
        .templateId(CAR_ENCODER.sbeTemplateId())
        .schemaId(CAR_ENCODER.sbeSchemaId())
        .version(CAR_ENCODER.sbeSchemaVersion());

    bufferOffset += MESSAGE_HEADER_ENCODER.encodedLength();

    CAR_ENCODER.wrap(MDB_VAL, bufferOffset);

    final int srcOffset = 0;
    CAR_ENCODER.serialNumber(serialNumber)
        .modelYear(MODEL_YEAR)
        .available(AVAILABLE)
        .code(CODE)
        .putVehicleCode(VEHICLE_CODE, srcOffset);

    for (int i = 0, size = CarEncoder.someNumbersLength(); i < size; i++) {
      CAR_ENCODER.someNumbers(i, i);
    }

    CAR_ENCODER.extras()
        .clear()
        .cruiseControl(true)
        .sportsPack(true)
        .sunRoof(false);

    CAR_ENCODER.engine()
        .capacity(CAPACITY)
        .numCylinders((short) 4)
        .putManufacturerCode(MANUFACTURER_CODE, srcOffset)
        .booster().boostType(BoostType.NITROUS).horsePower((short) 200);

    CAR_ENCODER.fuelFiguresCount(3)
        .next().speed(30).mpg(35.9f).usageDescription("Urban Cycle")
        .next().speed(55).mpg(49.0f).usageDescription("Combined Cycle")
        .next().speed(75).mpg(40.0f).usageDescription("Highway Cycle");

    final CarEncoder.PerformanceFiguresEncoder perfFigures = CAR_ENCODER.performanceFiguresCount(2);
    perfFigures.next()
        .octaneRating((short) 95)
        .accelerationCount(3)
        .next().mph(30).seconds(4.0f)
        .next().mph(60).seconds(7.5f)
        .next().mph(100).seconds(12.2f);
    perfFigures.next()
        .octaneRating((short) 99)
        .accelerationCount(3)
        .next().mph(30).seconds(3.8f)
        .next().mph(60).seconds(7.1f)
        .next().mph(100).seconds(11.8f);

    CAR_ENCODER.make(new String(MAKE, StandardCharsets.UTF_8))
        .putModel(MODEL, srcOffset, MODEL.length)
        .putActivationCode(ACTIVATION_CODE, 0, ACTIVATION_CODE.capacity());
  }

  private void decodeSbe(long serialNumber) {
    int bufferOffset = 0;
    MESSAGE_HEADER_DECODER.wrap(MDB_VAL, bufferOffset);
    
    final int templateId = MESSAGE_HEADER_DECODER.templateId();
    assertThat(templateId, is(CarEncoder.TEMPLATE_ID));

    final int actingBlockLength = MESSAGE_HEADER_DECODER.blockLength();
    assertThat(actingBlockLength, is(CarEncoder.BLOCK_LENGTH));
    
    final int schemaId = MESSAGE_HEADER_DECODER.schemaId();
    assertThat(schemaId, is(CarEncoder.SCHEMA_ID));
    
    final int actingVersion = MESSAGE_HEADER_DECODER.version();
    assertThat(actingVersion, is(CarEncoder.SCHEMA_VERSION));

    bufferOffset += MESSAGE_HEADER_DECODER.encodedLength();
    CAR_DECODER.wrap(MDB_VAL, bufferOffset, actingBlockLength, actingVersion);

    final byte[] buffer = new byte[128];

    assertThat(CAR_DECODER.serialNumber(), is(serialNumber));
    assertThat(CAR_DECODER.modelYear(), is(MODEL_YEAR));
    assertThat(CAR_DECODER.available(), is(AVAILABLE));
    assertThat(CAR_DECODER.code(), is(CODE));

    for (int i = 0, size = CarEncoder.someNumbersLength(); i < size; i++) {
      assertThat((int) CAR_DECODER.someNumbers(i), is(i));
    }

    final OptionalExtrasDecoder extras = CAR_DECODER.extras();
    assertThat(extras.cruiseControl(), is(true));
    assertThat(extras.sportsPack(), is(true));
    assertThat(extras.sunRoof(), is(false));

    final EngineDecoder engine = CAR_DECODER.engine();
    assertThat(engine.capacity(), is(CAPACITY));

    for (final FuelFiguresDecoder fuelFigures : CAR_DECODER.fuelFigures()) {
      assertThat(fuelFigures.count(), is(3));
      fuelFigures.usageDescription();
    }
    
    for (final PerformanceFiguresDecoder perfFigures : CAR_DECODER.performanceFigures()) {
      perfFigures.octaneRating();
      for (AccelerationDecoder acceleration : perfFigures.acceleration()) {
        acceleration.mph();
      }
    assertThat(perfFigures.count(), is(2));
    }

    assertThat(CAR_DECODER.make(), is(new String(MAKE, StandardCharsets.UTF_8)));
  }
}
