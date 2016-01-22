import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class MarketIndex implements Writable {

    private String name;
    public Date date;
    public float closingValue;
    public float dailyVariation;
    public float openingValue;
    public float higherValue;
    public float lowerValue;
    public float annualVariation;
    public int capitalization;

    public MarketIndex(String name, Date date, float closingValue, float dailyVariation, float openingValue, float higherValue, float lowerValue, float annualVariation, int capitalization) {
        this.name = name;
        this.date = date;
        this.closingValue = closingValue;
        this.dailyVariation = dailyVariation;
        this.openingValue = openingValue;
        this.higherValue = higherValue;
        this.lowerValue = lowerValue;
        this.annualVariation = annualVariation;
        this.capitalization = capitalization;
    }

    public MarketIndex() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeLong(date.getTime());
        dataOutput.writeFloat(closingValue);
        dataOutput.writeFloat(dailyVariation);
        dataOutput.writeFloat(openingValue);
        dataOutput.writeFloat(higherValue);
        dataOutput.writeFloat(lowerValue);
        dataOutput.writeFloat(annualVariation);
        dataOutput.writeInt(capitalization);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if (dataInput != null) {
            name = dataInput.readUTF();
            date = new Date(dataInput.readLong());
            closingValue = dataInput.readFloat();
            dailyVariation = dataInput.readFloat();
            openingValue = dataInput.readFloat();
            higherValue = dataInput.readFloat();
            lowerValue = dataInput.readFloat();
            annualVariation = dataInput.readFloat();
            capitalization = dataInput.readInt();
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public float getClosingValue() {
        return closingValue;
    }

    public void setClosingValue(float closingValue) {
        this.closingValue = closingValue;
    }

    public float getDailyVariation() {
        return dailyVariation;
    }

    public void setDailyVariation(float dailyVariation) {
        this.dailyVariation = dailyVariation;
    }

    public float getOpeningValue() {
        return openingValue;
    }

    public void setOpeningValue(float openingValue) {
        this.openingValue = openingValue;
    }

    public float getHigherValue() {
        return higherValue;
    }

    public void setHigherValue(float higherValue) {
        this.higherValue = higherValue;
    }

    public float getLowerValue() {
        return lowerValue;
    }

    public void setLowerValue(float lowerValue) {
        this.lowerValue = lowerValue;
    }

    public float getAnnualVariation() {
        return annualVariation;
    }

    public void setAnnualVariation(float annualVariation) {
        this.annualVariation = annualVariation;
    }

    public int getCapitalization() {
        return capitalization;
    }

    public void setCapitalization(int capitalization) {
        this.capitalization = capitalization;
    }

    @Override
    public String toString() {
        return "MarketIndex{" +
                "name='" + name + '\'' +
                ", date=" + date +
                ", closingValue=" + closingValue +
                ", dailyVariation=" + dailyVariation +
                ", openingValue=" + openingValue +
                ", higherValue=" + higherValue +
                ", lowerValue=" + lowerValue +
                ", annualVariation=" + annualVariation +
                ", capitalization=" + capitalization +
                '}';
    }
}
