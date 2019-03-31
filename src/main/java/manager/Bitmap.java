package manager;

import beans.BitmapEntryBean;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

public class Bitmap {

    /*
     * Operations:
     * 1. Fetch byte array of a sensor
     * 2. Start time of bitmap
     * 3. Granularity of bitmap
     * 4. Fetch range of bits from byte array of a particular sensor
     * 5. Find TS range of non-existing data within a range of bits
     *
     * */

    final Long startTime = 1451586600l; // Fri Jan 01 2016 00:00:00
    final Long endTime = 1609439400l; // Fri Jan 01 2021 00:00:00
    HashMap<String, BitmapEntryBean> bitmapEntries = new HashMap<>();
    String granularity;

    public Bitmap(List<String> sensorIds, String granularity) {
        this.granularity = granularity;
        for (String sensorId : sensorIds) {
            int nbits = 0;
            if (granularity.equalsIgnoreCase("1 minute")) {
                nbits = (int) (endTime - startTime) / 60;
            } else if (granularity.equalsIgnoreCase("1 hour")) {
                nbits = (int) (endTime - startTime) / 60 / 60;
            } else if (granularity.equalsIgnoreCase("1 day")) {
                nbits = (int) (endTime - startTime) / 60 / 60 / 24;
            }
            BitSet bitSet = new BitSet(nbits);
            bitSet.clear();
            bitmapEntries.put(sensorId, new BitmapEntryBean(sensorId, bitSet));
        }
    }

    public ArrayList<Query.TimeRange> getNonExistingDataRange(String sensorId, Query.TimeRange timeRange) throws Exception {
        BitmapEntryBean bitmapEntry = bitmapEntries.get(sensorId);
        if (bitmapEntry == null) throw new Exception("No bitmap entry found for sensor id: " + sensorId);
        BitSet bitSet = bitmapEntry.getBitSets();
        ArrayList<Query.TimeRange> nonExistingTimeRanges = new ArrayList<>();
        int startI = (int) (timeRange.startTime - this.startTime);
        int endI = (int) (timeRange.endTime - this.startTime);
        int progressI = startI;
        while (progressI < endI) {
            int clearBitI = bitSet.nextClearBit(progressI);
            if (clearBitI == -1 || clearBitI >= endI) break;
            int setBitI = bitSet.nextSetBit(clearBitI);
            if (setBitI == -1 || setBitI > endI) {
                setBitI = endI;
            }
            nonExistingTimeRanges.add(new Query.TimeRange(clearBitI + this.startTime, setBitI + this.startTime));
            progressI = setBitI;
        }
        return nonExistingTimeRanges;
    }

    public void updateBitmap(Query query, String sensorId, ArrayList<Query.TimeRange> timeRanges) {
        BitmapEntryBean bitmapEntry = bitmapEntries.get(sensorId);
        for (Query.TimeRange timeRange : timeRanges) {
            bitmapEntry.getBitSets().set((int) (timeRange.startTime - this.startTime), (int) (timeRange.endTime - this.startTime));
            assert (bitmapEntry.getBitSets().get((timeRange.startTime).intValue()) == true);
        }
    }
}
