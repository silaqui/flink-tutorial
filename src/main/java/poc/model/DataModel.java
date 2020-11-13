package poc.model;

public class DataModel {


    public int id;
    public String data;
    public int multiply;

    public DataModel(int id, String data, int multiply) {
        this.id = id;
        this.data = data;
        this.multiply = multiply;
    }

    @Override
    public String toString() {
        return "DataModel{" +
                "id=" + id +
                ", data='" + data + '\'' +
                ", multiply=" + multiply +
                '}';
    }
}
