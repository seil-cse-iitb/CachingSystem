package beans;

import java.io.Serializable;
import java.util.BitSet;

public class BitmapEntryBean implements Serializable {
    String sensorId;
    BitSet bitSets;

    public BitmapEntryBean() {
    }

    public BitmapEntryBean(String sensorId, BitSet bitSets) {
        this.sensorId = sensorId;
        this.bitSets = bitSets;
    }


    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public BitSet getBitSets() {
        return bitSets;
    }

    public void setBitSets(BitSet bitSets) {
        this.bitSets = bitSets;
    }

}
