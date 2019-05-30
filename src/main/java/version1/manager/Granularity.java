package version1.manager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Granularity implements Serializable {
    public static List<Granularity> granularities = new ArrayList<>();
    private static Comparator<Granularity> granularityComparator = Comparator.comparingInt(o -> o.displayPriority);
    private final String name;
    private final Integer granularityInTermsOfSeconds;
    private final Integer displayLimitInSeconds;
    private final Integer displayPriority; //This parameter gives ordering in which data will be sent to visualisation portal
    // this will generally be in increasing order of displayLimitInSeconds
    private final Integer fetchIntervalAtOnceInSeconds;
    private final Integer numPartitionsForEachInterval;

    public Granularity(String name, Integer granularityInTermsOfSeconds, Integer displayLimitInSeconds, Integer displayPriority, Integer fetchIntervalAtOnceInSeconds, Integer numPartitionsForEachInterval) {
        this.name = name;
        this.granularityInTermsOfSeconds = granularityInTermsOfSeconds;
        this.displayLimitInSeconds = displayLimitInSeconds;
        this.displayPriority = displayPriority;
        this.fetchIntervalAtOnceInSeconds = fetchIntervalAtOnceInSeconds;
        this.numPartitionsForEachInterval = numPartitionsForEachInterval;
        granularities.add(this);
    }

    public static Granularity eligibleGranularity(List<Query.TimeRange> timeRanges) {
        long totalTime = 0;
        for (Query.TimeRange timeRange : timeRanges) {
            totalTime += timeRange.endTime - timeRange.startTime;
        }
        granularities.sort(granularityComparator);
        for (Granularity granularity : granularities) {
            if (totalTime <= granularity.displayLimitInSeconds) {
                return granularity;
            }
        }
        return granularities.get(granularities.size() - 1);
    }

    public Integer getDisplayPriority() {
        return displayPriority;
    }

    public String getName() {
        return name;
    }

    public String getAsDisplayString() {
        return displayPriority + " (" + name + ")";
    }

    public Integer getGranularityInTermsOfSeconds() {
        return granularityInTermsOfSeconds;
    }

    public Integer getDisplayLimitInSeconds() {
        return displayLimitInSeconds;
    }

    public Integer getNumPartitionsForEachInterval() {
        return numPartitionsForEachInterval;
    }

    public Integer getFetchIntervalAtOnceInSeconds() {
        return fetchIntervalAtOnceInSeconds;
    }

    @Override
    public String toString() {
        return "Granularity{" +
                "name='" + name + '\'' +
                ", granularityInTermsOfSeconds=" + granularityInTermsOfSeconds +
                ", displayLimitInSeconds=" + displayLimitInSeconds +
                ", displayPriority=" + displayPriority +
                ", fetchIntervalAtOnceInSeconds=" + fetchIntervalAtOnceInSeconds +
                ", numPartitionsForEachInterval=" + numPartitionsForEachInterval +
                '}';
    }
}

