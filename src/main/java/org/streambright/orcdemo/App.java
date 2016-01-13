package org.streambright.orcdemo;

import org.apache.hadoop.hive.ql.io.orc.OrcRecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

import org.apache.hadoop.conf.Configuration;

//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.UUID;


public class App {

    public static Configuration conf = new Configuration();
    public static Writer writer;
    public static OrcFile.WriterOptions orc_options = OrcFile.writerOptions(conf);
    public static OrcFile.Version vers = OrcFile.Version.V_0_12;
    public static CompressionKind compr = CompressionKind.ZLIB;

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

        String path = "/tmp/orcfile.orc";

        try {

            conf = new Configuration();
            //FileSystem fs = FileSystem.getLocal(conf);

            ObjectInspector ObjInspector = ObjectInspectorFactory.
                    getReflectionObjectInspector(OrcRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

            Path local_path = new Path(path);

            orc_options.inspector(ObjInspector).
                    stripeSize(8388608).
                    bufferSize(8388608).
                    blockPadding(true).
                    // bloomFilterColumns("col1").
                    compress(compr).
                    version(vers);

            writer = OrcFile.createWriter(local_path, orc_options);

            for (int i=1; i<1100000; i++) {
                writer.addRow(new OrcRow(i, UUID.randomUUID().toString(), "orcFile"));
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
