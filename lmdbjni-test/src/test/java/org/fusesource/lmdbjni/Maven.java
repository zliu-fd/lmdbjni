package org.fusesource.lmdbjni;

import org.fusesource.hawtjni.runtime.Library;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;

import java.io.File;
import java.net.URL;

public class Maven {
    public static String classPath;

    static {
        File hawtjni = Maven.getClassPath(Library.class);
        File jmh = Maven.getClassPath(GenerateMicroBenchmark.class);
        File lmdbjni = Maven.getClassPath(Database.class);
        File lmdbjnitest = Maven.getClassPath(PerfTest1.class);
        File lmdbjnilinux64 = Maven.findTargetJar("lmdbjni-linux64");
        File lmdbjniosx64 = Maven.findTargetJar("lmdbjni-osx64");
        classPath = Maven.createClassPath(hawtjni, jmh, lmdbjni, lmdbjnitest, lmdbjnilinux64, lmdbjniosx64);
    }

    public static File getClassPath(Class<?> anyTestClass) {
        final String clsUri = anyTestClass.getName().replace('.', '/') + ".class";
        final URL url = anyTestClass.getClassLoader().getResource(clsUri);
        final String clsPath = url.getPath();
        if (clsPath.contains("!")) {
            // 3pp jar deps
            return new File(clsPath.substring(5, clsPath.length() - clsUri.length() - 2));
        } else {
            int offset = 14;
            // local maven target classes
            int idx = clsPath.lastIndexOf("target/classes");
            if (idx == -1) {
                idx = clsPath.lastIndexOf("target/test-classes");
                offset = 19;
            }
            return new File(clsPath.substring(0, idx + offset));
        }
    }

    public static File findTargetJar(String subProject) {
        File root = new File(Maven.getClassPath(Maven.class)
                .getParentFile()
                .getParentFile()
                .getParentFile(), subProject + "/target");
        String[] list = root.list();
        if (list == null) {
            return null;
        }
        for (String f : list) {
            if (f.endsWith("jar") && !f.contains("sources")) {
                return new File(root, f);
            }
        }
        throw new IllegalArgumentException("Could not find jar for " + subProject);
    }

    public static String createClassPath(File... files) {
        StringBuilder sb = new StringBuilder();
        for (File f : files) {
            if (f != null) {
                sb.append(":").append(f.getAbsolutePath());
            }
        }
        return sb.toString();
    }

    public static void recreateDir(File dir) {
        // delete one level.
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null)
                for (File file : files)
                    if (file.isDirectory()) {
                        recreateDir(file);
                    } else {
                        file.delete();
                    }
        }
        dir.delete();
        dir.mkdirs();
    }
}
