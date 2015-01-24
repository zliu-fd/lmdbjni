/**
 * Copyright (C) 2013, RedHat, Inc.
 *
 *    http://www.redhat.com/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fusesource.lmdbjni;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.LinkedList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.fusesource.lmdbjni.Constants.*;

/**
 * Unit tests for the LMDB API.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class EnvTest {
    static {
        Setup.setLmdbLibraryPath();
    }

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Rule
    public TemporaryFolder backup = new TemporaryFolder();

    @Test
    public void testCRUD() throws Exception {
        String path = tmp.newFolder().getCanonicalPath();
        try (Env env = new Env()) {
            env.open(path);
            try (Database db = env.openDatabase()) {
                doTest(env, db);
            }
        }
    }

    @Test
    public void testBackup() throws Exception {
        String path = tmp.newFolder().getCanonicalPath();
        String backupPath = backup.newFolder().getCanonicalPath();
        try (Env env = new Env()) {
            env.open(path);
            try (Database db = env.openDatabase()) {
                db.put(new byte[] {1}, new byte[] {1});
                env.copy(backupPath);
            }
        }
        try (Env env = new Env()) {
            env.open(backupPath);
            try (Database db = env.openDatabase()) {
                byte[] value = db.get(new byte[]{1});
                assertThat((int) value[0], is(1));
            }
        }
    }

    @Test
    public void testBackupCompact() throws Exception {
        String path = tmp.newFolder().getCanonicalPath();
        String backupPath = backup.newFolder().getCanonicalPath();
        try (Env env = new Env()) {
            env.open(path);
            try (Database db = env.openDatabase()) {
                db.put(new byte[] {1}, new byte[] {1});
                env.copyCompact(backupPath);
            }
        }
        try (Env env = new Env()) {
            env.open(backupPath);
            try (Database db = env.openDatabase()) {
                byte[] value = db.get(new byte[]{1});
                assertThat((int) value[0], is(1));
            }
        }
    }

    @Test
    public void testEnvInfo() throws Exception {
        String path = tmp.newFolder().getCanonicalPath();
        try (Env env = new Env()) {
            env.open(path);
            env.setMapSize(1048576L);
            try (Database db = env.openDatabase()) {
                db.put(new byte[] {1}, new byte[] {1});
                EnvInfo info = env.info();
                assertThat(info.getMapSize(), is(1048576L));
            }
        }
    }

    @Test
    public void testStat() throws Exception {
        String path = tmp.newFolder().getCanonicalPath();
        try (Env env = new Env()) {
            env.open(path);
            env.setMapSize(1048576L);
            try (Database db = env.openDatabase()) {
                db.put(new byte[] {1}, new byte[] {1});
                db.put(new byte[] {2}, new byte[] {1});
                Stat stat = env.stat();
                assertThat(stat.getEntries(), is(2L));
            }
        }
    }

    @Test
    public void testMaxKeySize() throws Exception {
        String path = tmp.newFolder().getCanonicalPath();
        try (Env env = new Env()) {
            env.open(path);
            assertThat(env.getMaxKeySize(), is(511L));
        }
    }


    private void doTest(Env env, Database db) {
        assertNull(db.put(bytes("Tampa"), bytes("green")));
        assertNull(db.put(bytes("London"), bytes("red")));

        assertNull(db.put(bytes("New York"), bytes("gray")));
        assertNull(db.put(bytes("New York"), bytes("blue")));
        assertArrayEquals(db.put(bytes("New York"), bytes("silver"), NOOVERWRITE), bytes("blue"));

        assertArrayEquals(db.get(bytes("Tampa")), bytes("green"));
        assertArrayEquals(db.get(bytes("London")), bytes("red"));
        assertArrayEquals(db.get(bytes("New York")), bytes("blue"));

        Transaction tx = env.createTransaction();
        try {
            // Lets verify cursoring works..
            LinkedList<String> keys = new LinkedList<>();
            LinkedList<String> values = new LinkedList<>();

            try (Cursor cursor = db.openCursor(tx)) {
                for (Entry entry = cursor.get(FIRST); entry != null; entry = cursor.get(NEXT)) {
                    keys.add(string(entry.getKey()));
                    values.add(string(entry.getValue()));
                }
            }
            assertEquals(Arrays.asList(new String[] { "London", "New York", "Tampa" }), keys);
            assertEquals(Arrays.asList(new String[] { "red", "blue", "green" }), values);
        } finally {
            tx.commit();
        }

        assertTrue(db.delete(bytes("New York")));
        assertNull(db.get(bytes("New York")));

        // We should not be able to delete it again.
        assertFalse(db.delete(bytes("New York")));

        // put /w readonly transaction should fail.
        tx = env.createTransaction(true);
        try {
            db.put(tx, bytes("New York"), bytes("silver"));
            fail("Expected LMDBException");
        } catch (LMDBException e) {
            assertTrue(e.getErrorCode() > 0);
        }
    }
}
