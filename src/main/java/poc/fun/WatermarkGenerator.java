package poc.fun;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import poc.model.CountedDataModel;

import javax.annotation.Nullable;

public class WatermarkGenerator implements AssignerWithPunctuatedWatermarks<CountedDataModel> {

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(CountedDataModel lastElement, long extractedTimestamp) {
        return lastElement.isLast ? new Watermark(extractedTimestamp) : null;
    }

    @Override
    public long extractTimestamp(CountedDataModel element, long previousElementTimestamp) {
        return element.timestamp;
    }
}