import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MarketIndex implements Writable {

    private String name;
    private long date;
    private float closingValue;
    private float dailyVariation;
    private float openingValue;
    private float higherValue;
    private float lowerValue;
    private float annualVariation;
    private int dailyExchangeVolume;

    public MarketIndex(String name, long date, float closingValue, float dailyVariation, float openingValue, float higherValue, float lowerValue, float annualVariation, int dailyExchangeVolume) {
        this.name = name;
        this.date = date;
        this.closingValue = closingValue;
        this.dailyVariation = dailyVariation;
        this.openingValue = openingValue;
        this.higherValue = higherValue;
        this.lowerValue = lowerValue;
        this.annualVariation = annualVariation;
        this.dailyExchangeVolume = dailyExchangeVolume;
    }

    public MarketIndex() {
    }
    
    public MarketIndex(MarketIndex copy) {
    	this.name = copy.getName();
    	this.date = copy.getDate();
    	this.closingValue = copy.getClosingValue();
    	this.dailyVariation = copy.getDailyVariation();
    	this.openingValue = copy.getOpeningValue();
        this.higherValue = copy.getHigherValue();
        this.lowerValue = copy.getLowerValue();
        this.annualVariation = copy.getAnnualVariation();
        this.dailyExchangeVolume = copy.getDailyExchangeVolume();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeLong(date);
        dataOutput.writeFloat(closingValue);
        dataOutput.writeFloat(dailyVariation);
        dataOutput.writeFloat(openingValue);
        dataOutput.writeFloat(higherValue);
        dataOutput.writeFloat(lowerValue);
        dataOutput.writeFloat(annualVariation);
        dataOutput.writeInt(dailyExchangeVolume);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if (dataInput != null) {
            name = dataInput.readUTF();
            date = dataInput.readLong();
            closingValue = dataInput.readFloat();
            dailyVariation = dataInput.readFloat();
            openingValue = dataInput.readFloat();
            higherValue = dataInput.readFloat();
            lowerValue = dataInput.readFloat();
            annualVariation = dataInput.readFloat();
            dailyExchangeVolume = dataInput.readInt();
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
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

    public int getDailyExchangeVolume() {
        return dailyExchangeVolume;
    }

    public void setDailyExchangeVolume(int dailyExchangeVolume) {
        this.dailyExchangeVolume = dailyExchangeVolume;
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
                ", dailyExchangeVolume=" + dailyExchangeVolume +
                '}';
    }
}
