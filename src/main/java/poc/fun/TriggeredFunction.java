package poc.fun;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import poc.model.CountedDataModel;

public class TriggeredFunction extends KeyedProcessFunction<Integer, CountedDataModel, CountedDataModel> {
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<CountedDataModel> out) throws Exception {
        long l = ctx.timerService().currentWatermark();
        System.out.println("TriggeredFunction - onTimer " + l);
    }

    @Override
    public void processElement(CountedDataModel countedDataModel, Context context, Collector<CountedDataModel> collector) throws Exception {
        context.timerService().registerEventTimeTimer(0L);
//        System.out.println("TriggeredFunction: " + countedDataModel);
        collector.collect(countedDataModel);
    }

}
