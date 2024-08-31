import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvfOutlierDetectionJob1Driver {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: AvfOutlierDetectionJob1Driver [INPUT PATH] [OUTPUT PATH] [FIELD COLUMN NO]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("field.no", args[2]);

        Job job = Job.getInstance(conf, "AVF Frequency Calculation");
        job.setJarByClass(AvfOutlierDetectionJob1Driver.class);

        job.setMapperClass(AvfOutlierDetectionJob1Mapper.class);
        job.setReducerClass(AvfOutlierDetectionJob1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
