package poc;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import poc.model.CountedDataModel;
import poc.model.MultiplyDataModel;

public class MultiplyFun extends KeyedProcessFunction<Integer, CountedDataModel, MultiplyDataModel> {

    @Override
    public void processElement(CountedDataModel value, Context ctx, Collector<MultiplyDataModel> out) throws Exception {

        for (int i = 0; i < value.data.multiply; i++) {
            if (value.data.multiply - 1 == i && value.isLast) {
                out.collect(new MultiplyDataModel(value.data.id, value.data.data, true));
            } else {
                out.collect(new MultiplyDataModel(value.data.id, value.data.data, false));
            }
        }
    }
}
