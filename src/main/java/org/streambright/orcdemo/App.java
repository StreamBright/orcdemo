package org.streambright.orcdemo;

import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


public class App {

    private static Configuration conf = new Configuration();
    public static Writer writer;

    public static class OrcRow {
        int col1;
        String col2;
        String col3;

        OrcRow(int a, String b, String c) {
            this.col1 = a;
            this.col2 = b;
            this.col3 = c;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        String path = "/tmp/orcfile1";

        try {

            conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);

            ObjectInspector ObjInspector = ObjectInspectorFactory.getReflectionObjectInspector(OrcRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            writer = OrcFile.createWriter(new Path(path), OrcFile.writerOptions(conf).inspector(ObjInspector).stripeSize(100000).bufferSize(10000).compress(CompressionKind.ZLIB).version(OrcFile.Version.V_0_12));

            writer.addRow(new OrcRow(1, "hello", "orcFile"));
            writer.addRow(new OrcRow(2, "hello2", "orcFile2"));

            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
