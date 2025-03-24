import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HDFSFullExample {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            FileSystem fs = FileSystem.get(conf);

            // Create directory
            Path dir = new Path("/test");
            fs.mkdirs(dir);

            // Write file
            Path file = new Path(dir, "test1.txt");
            try (FSDataOutputStream out = fs.create(file)) {
                out.writeUTF("HDFS Java API Demo\n");
            }

            // Read file
            try (FSDataInputStream in = fs.open(file)) {
                System.out.println(in.readUTF());
            }

            // Cleanup
            //fs.delete(dir, true);
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
