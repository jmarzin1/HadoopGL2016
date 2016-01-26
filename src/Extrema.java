import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Extrema implements Writable{

    private MarketIndex best;
    private MarketIndex worst;

    public Extrema() {
        best = new MarketIndex();
        worst = new MarketIndex();
    }

    public Extrema(MarketIndex best, MarketIndex worst) {
        this.best = best;
        this.worst = worst;
    }

    public MarketIndex getBest() {
        return best;
    }

    public void setBest(MarketIndex best) {
        this.best = best;
    }

    public MarketIndex getWorst() {
        return worst;
    }

    public void setWorst(MarketIndex worst) {
        this.worst = worst;
    }

    @Override
    public String toString() {
        return "Extrema{" +
                "best=" + best +
                ", worst=" + worst +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        best.write(dataOutput);
        worst.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        best.readFields(dataInput);
        worst.readFields(dataInput);
    }


}
