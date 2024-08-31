import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvfOutlierDetectionJob1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private int fieldNo;
    private static final IntWritable one = new IntWritable(1);
    private Text attributeValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Retrieve the field index from the configuration
        fieldNo = Integer.parseInt(context.getConfiguration().get("field.no"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the CSV line
        String[] fields = value.toString().split(",");

        // Extract the target attribute value
        if (fields.length > fieldNo) {
            Integer attrValueInt = (int) Double.parseDouble(fields[fieldNo]); // Cast to integer part of double
            attributeValue.set(Integer.toString(attrValueInt));
            context.write(attributeValue, one);
        }
    }
}
