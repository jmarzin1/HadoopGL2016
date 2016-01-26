import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxTuple implements Writable{
    private float minVar;
    private float maxVar;

    public float getMinVar() {
        return minVar;
    }

    public void setMinVar(float minVar) {
        this.minVar = minVar;
    }

    public float getMaxVar() {
        return maxVar;
    }

    public void setMaxVar(float maxVar) {
        this.maxVar = maxVar;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(minVar);
        dataOutput.writeFloat(maxVar);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if (dataInput != null) {
            minVar = dataInput.readFloat();
            maxVar = dataInput.readFloat();
        }
    }

    @Override
    public String toString() {
        return "MinMaxTuple{" +
                ", minVar=" + minVar +
                ", maxVar=" + maxVar +
                '}';
    }
}
