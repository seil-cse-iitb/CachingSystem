package version1.manager;

import java.util.*;

public class Bitmap {
    public static boolean isDirty=false;
    final Date startDate, endDate;
    final Map<Granularity, BitSet> granularityBitSetMap = new HashMap<>();
    private final Map<String, Granularity> granularityMap = new HashMap<>();

    public Bitmap(Date startDate, Date endDate, List<Granularity> granularities) {
        this.startDate = startDate;
        this.endDate = endDate;
        for (Granularity granularity :
                granularities) {
            int bitsetSize = (int) ((getTimeInSec(endDate) - getTimeInSec(startDate)) / granularity.getGranularityInTermsOfSeconds());
            BitSet bitSet = new BitSet(bitsetSize);
            bitSet.clear();
            granularityBitSetMap.put(granularity, bitSet);
            granularityMap.put(granularity.getAsDisplayString(), granularity);
        }
    }

    public Collection<? extends Query.TimeRange> getNonExistingDataRange(Granularity granularity, Query.TimeRange timeRange) {
        BitSet bitSet = this.granularityBitSetMap.get(granularity);
        ArrayList<Query.TimeRange> nonExistingTimeRanges = new ArrayList<>();
        int startI = (int) (timeRange.startTime - getTimeInSec(startDate)) / granularity.getGranularityInTermsOfSeconds();
        int endI = (int) (timeRange.endTime - getTimeInSec(startDate)) / granularity.getGranularityInTermsOfSeconds();
        int progressI = startI;
        while (progressI < endI) {
            int clearBitI = bitSet.nextClearBit(progressI);
            if (clearBitI == -1 || clearBitI >= endI) break;
            int setBitI = bitSet.nextSetBit(clearBitI);
            if (setBitI == -1 || setBitI > endI) {
                setBitI = endI;
            }
            nonExistingTimeRanges.add(new Query.TimeRange(clearBitI * granularity.getGranularityInTermsOfSeconds() + getTimeInSec(startDate),
                    setBitI * granularity.getGranularityInTermsOfSeconds() + getTimeInSec(startDate)));
            progressI = setBitI;
        }
        return nonExistingTimeRanges;
    }

    public void updateBitmap(Granularity granularity, ArrayList<Query.TimeRange> timeRanges) {
        LogManager.logInfo("[Updating Bitmap][Granularity:" + granularity.getAsDisplayString() + "]");
        Bitmap.isDirty=true;
        BitSet bitSet = granularityBitSetMap.get(granularity);
        for (Query.TimeRange timeRange : timeRanges) {
            int start = (int) ((timeRange.startTime - getTimeInSec(startDate)) / granularity.getGranularityInTermsOfSeconds());
            int end = (int) (timeRange.endTime - getTimeInSec(startDate)) / granularity.getGranularityInTermsOfSeconds();
            bitSet.set(start, end);
        }
    }

    private long getTimeInSec(Date date) {
        return date.getTime() / 1000;
    }

    @Override
    public String toString() {
        return "Bitmap{" +
                "startDate=" + startDate +
                ", endDate=" + endDate +
                ", granularityBitSetMap.keySet()=" + granularityBitSetMap.keySet() +
                '}';
    }

    public void setBitset(String granularity, BitSet bitset) {
        granularityBitSetMap.put(granularityMap.get(granularity), bitset);
    }
}
