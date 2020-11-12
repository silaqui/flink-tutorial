package poc.model;

public class CountedDataModel {

    public DataModel data;
    public CountModel count;
    public Boolean isLast;
    public Long timestamp;

    public CountedDataModel(DataModel data, CountModel count, Boolean isLast, Long timestamp) {
        this.data = data;
        this.count = count;
        this.isLast = isLast;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "CountedDataModel{" +
                "data=" + data +
                ", count=" + count +
                ", isLast=" + isLast +
                ", timestamp=" + timestamp +
                '}';
    }

    public DataModel getData() {
        return data;
    }

    public void setData(DataModel data) {
        this.data = data;
    }

    public CountModel getCount() {
        return count;
    }

    public void setCount(CountModel count) {
        this.count = count;
    }

    public Boolean getLast() {
        return isLast;
    }

    public void setLast(Boolean last) {
        isLast = last;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
