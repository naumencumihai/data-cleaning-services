import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OutlierDetectionReducer extends Reducer<Text, Text, Text, Text> {

    private double stddevThreshold;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Retrieve the number of standard deviations from the configuration
        stddevThreshold = Double.parseDouble(context.getConfiguration().get("stddev.threshold"));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        ArrayList<Double> valueList = new ArrayList<>();
        double sum = 0;
        double sumSquares = 0;
        int count = 0;

        for (DoubleWritable val : values) {
            double v = val.get();
            sum += v;
            sumSquares += v * v;
            count++;
            valueList.add(v);
        }

        double mean = sum / count;
        double variance = (sumSquares / count) - (mean * mean);
        double stdDev = Math.sqrt(variance);

        String[] keyParts = key.toString().split(",");
        String rowIndex = keyParts[1];

        // Identify outliers
        for (double v : valueList) {
            String label = Math.abs(v - mean) > stddevThreshold * stdDev ? "OUT" : "---";
            String outputValue = String.format("%s,%s,", label, rowIndex);
            context.write(new Text(outputValue), new DoubleWritable(v));
        }
    }
}
