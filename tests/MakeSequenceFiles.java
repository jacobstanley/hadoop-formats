import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.SnappyCodec;

public class MakeSequenceFiles {
    public static void main(String args[]) throws IOException {
        System.loadLibrary("snappy");

        Configuration conf = new Configuration();
        Path path = new Path("./text-int.seq");
        SequenceFile.Writer writer = SequenceFile.createWriter(
            conf,
            Writer.file(path),
            Writer.keyClass(Text.class),
            Writer.valueClass(IntWritable.class),
            Writer.compression(CompressionType.BLOCK, new SnappyCodec()));

        for (int i = 0; i < 100000; i++) {
            String key = String.format("F%07d", i);
            int value = (int)Math.round(Math.random() * 100);
            writer.append(new Text(key), new IntWritable(value));
        }

        writer.close();
    }
}
