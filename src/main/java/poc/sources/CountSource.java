package poc.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import poc.model.CountModel;

public class  CountSource implements SourceFunction<CountModel> {

    private boolean isRunning = true;


    public void run(SourceContext<CountModel> sourceContext) throws Exception {
        sourceContext.collect(new CountModel(1,8));
        sourceContext.collect(new CountModel(2,10));
        while (isRunning){ }
    }

    public void cancel() {
        System.out.println("CountSource cancel");
        isRunning = false;
    }
}