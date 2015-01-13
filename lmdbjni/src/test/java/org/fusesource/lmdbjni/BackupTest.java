package org.fusesource.lmdbjni;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BackupTest {
    static {
        Setup.setLmdbLibraryPath();
    }

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Rule
    public TemporaryFolder backup = new TemporaryFolder();

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
}
