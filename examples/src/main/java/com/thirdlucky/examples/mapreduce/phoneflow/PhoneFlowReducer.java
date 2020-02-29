package com.thirdlucky.examples.mapreduce.phoneflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneFlowReducer extends Reducer<Text, PhoneFlow, Text, PhoneFlow> {
    private static PhoneFlow phoneFlow = new PhoneFlow();

    @Override
    protected void reduce(Text key, Iterable<PhoneFlow> values, Context context) throws IOException, InterruptedException {
        long upflow = 0;
        long downFlow = 0;

        while (values.iterator().hasNext()) {
            PhoneFlow pf = values.iterator().next();
            upflow += pf.getUpFlow();
            downFlow += pf.getDownFlow();
        }

        phoneFlow.set(upflow, downFlow);
        context.write(key, phoneFlow);
    }
}
