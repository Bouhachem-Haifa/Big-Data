package tn.isima.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer
        extends Reducer< Text, DoubleWritable,Text,DoubleWritable> {

    private IntWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        for (DoubleWritable val : values) {
            System.out.println("value: "+val.get());
            sum += val.get();
        }
        System.out.println("--> Sum = "+sum);
        result.set(sum);
        context.write(key, result);
    }
}
