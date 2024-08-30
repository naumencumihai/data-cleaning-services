import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OutlierDetectionMapper extends Mapper<LongWritable, Text, Text, Text> {

    private int fieldNo;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Retrieve specified field index
        fieldNo = Integer.parseInt(context.getConfiguration().get("field.no"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        // fields[0] represents the row index/id in the datasource, this format MUST be respected
        // String outputKey = "data," + fields[0]; 

        String dataValue = String.format("%d,%f",fields[0], Double.parseDouble(fields[fieldNo]));
        
        context.write(new Text("data"), new Text(dataValue));
    }
}
