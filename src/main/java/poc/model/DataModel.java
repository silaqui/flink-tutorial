package poc.model;

public class DataModel {


    public int id;
    public String data;

    public DataModel(int id, String data) {
        this.id = id;
        this.data = data;
    }

    @Override
    public String toString() {
        return "Data - Id: " + id + " - Data: " + data;
    }
}
