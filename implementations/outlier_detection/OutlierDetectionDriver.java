import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class OutlierDetectionDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: OutlierDetectionDriver [INPUT PATH] [OUTPUT PATH] [STDDEV THRESHOLD] [FIELD COLUMN NO]");
            System.exit(-1);
        }

        // TODO output: Key = index, value = label

        Configuration conf = new Configuration();
        conf.set("stddev.threshold", args[2]);  // Pass the stddev threshold to the configuration
        conf.set("field.no", args[3]); // Pass field no to the configuration

        Job job = Job.getInstance(conf, "Outlier Detection");
        job.setJarByClass(OutlierDetectionDriver.class);

        job.setMapperClass(OutlierDetectionMapper.class);
        job.setReducerClass(OutlierDetectionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
