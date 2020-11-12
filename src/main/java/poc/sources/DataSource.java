package poc.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import poc.model.DataModel;

public class DataSource implements SourceFunction<DataModel> {

    private boolean isRunning = true;

    public void run(SourceFunction.SourceContext<DataModel> sourceContext) throws Exception {
        DataModel m1 = new DataModel(1, "1a");
        DataModel m2 = new DataModel(1, "1b");
        DataModel m3 = new DataModel(1, "1c");
        DataModel m4 = new DataModel(1, "1d");
        DataModel m5 = new DataModel(1, "1e");
        DataModel m6 = new DataModel(1, "1f");
        DataModel m7 = new DataModel(1, "1g");
        DataModel m8 = new DataModel(1, "1h");

        DataModel m21 = new DataModel(2, "2a");
        DataModel m22 = new DataModel(2, "2b");
        DataModel m23 = new DataModel(2, "2c");
        DataModel m24 = new DataModel(2, "2d");
        DataModel m25 = new DataModel(2, "2e");
        DataModel m26 = new DataModel(2, "2f");
        DataModel m27 = new DataModel(2, "2g");
        DataModel m28 = new DataModel(2, "2h");
        DataModel m29 = new DataModel(2, "2i");
        DataModel m30 = new DataModel(2, "2j");

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

//        while (isRunning){ }
    }

    public void cancel() {
        System.out.println("DataSource cancel");
        isRunning = false;
    }
}
