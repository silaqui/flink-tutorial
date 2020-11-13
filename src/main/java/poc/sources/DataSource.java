package poc.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import poc.model.DataModel;

public class DataSource implements SourceFunction<DataModel> {

    private boolean isRunning = true;

    public void run(SourceFunction.SourceContext<DataModel> sourceContext) throws Exception {
        DataModel m1 = new DataModel(1, "1a", 2);
        DataModel m2 = new DataModel(1, "1b", 2);
        DataModel m3 = new DataModel(1, "1c", 2);
        DataModel m4 = new DataModel(1, "1d",1);
        DataModel m5 = new DataModel(1, "1e",1);
        DataModel m6 = new DataModel(1, "1f",1);
        DataModel m7 = new DataModel(1, "1g",1);
        DataModel m8 = new DataModel(1, "1h",1);

        DataModel m21 = new DataModel(2, "2a",2);
        DataModel m22 = new DataModel(2, "2b",2);
        DataModel m23 = new DataModel(2, "2c",1);
        DataModel m24 = new DataModel(2, "2d",1);
        DataModel m25 = new DataModel(2, "2e",1);
        DataModel m26 = new DataModel(2, "2f",1);
        DataModel m27 = new DataModel(2, "2g",1);
        DataModel m28 = new DataModel(2, "2h",1);
        DataModel m29 = new DataModel(2, "2i",1);
        DataModel m30 = new DataModel(2, "2j",1);

        sourceContext.collect(m1);
        sourceContext.collect(m2);
        sourceContext.collect(m3);
        sourceContext.collect(m4);
        sourceContext.collect(m5);
        sourceContext.collect(m6);
        sourceContext.collect(m7);
        sourceContext.collect(m8);

        sourceContext.collect(m21);
        sourceContext.collect(m22);
        sourceContext.collect(m23);
        sourceContext.collect(m24);
        sourceContext.collect(m25);
        sourceContext.collect(m26);
        sourceContext.collect(m27);
        sourceContext.collect(m28);
        sourceContext.collect(m29);
        sourceContext.collect(m30);

        while (isRunning){ }
    }

    public void cancel() {
        System.out.println("DataSource cancel");
        isRunning = false;
    }
}
