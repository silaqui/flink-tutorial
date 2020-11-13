package poc.model;

public class MultiplyDataModel {


    public int id;
    public String data;
    public Boolean isLast;

    public MultiplyDataModel(int id, String data, Boolean isLast) {
        this.id = id;
        this.data = data;
        this.isLast = isLast;
    }

    @Override
    public String toString() {
        return "DataModel{" +
                "id=" + id +
                ", data='" + data + '\'' +
                '}';
    }
}
