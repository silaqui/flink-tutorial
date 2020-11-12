package poc.model;

public class CountModel {


    public int id;
    public int count;

    public CountModel(int id, int count) {
        this.id = id;
        this.count = count;
    }

    @Override
    public String toString() {
        return "Count - Id: " + id + " - Expected Count: " + count;
    }
}
