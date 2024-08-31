import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvfOutlierDetectionJob1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable totalCount = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        totalCount.set(sum);
        context.write(key, totalCount);
    }
}
