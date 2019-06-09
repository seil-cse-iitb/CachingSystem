package beans;

import java.io.Serializable;

public class GranularityBean implements Serializable {
    String granularityId,windowDuration;
    Integer granularityInTermsOfSeconds;
    Integer displayLimitInSeconds;
    Integer fetchIntervalAtOnceInSeconds;
    Integer numPartitionsForEachInterval;
    Integer displayPriority; //This parameter gives ordering in which data will be sent to visualisation portal
    // this will generally be in increasing order of displayLimitInSeconds
    Integer numParallelQuery;

    public Integer getNumParallelQuery() {
        return numParallelQuery;
    }

    public void setNumParallelQuery(Integer numParallelQuery) {
        this.numParallelQuery = numParallelQuery;
    }

    public String getGranularityId() {
        return granularityId;
    }

    public void setGranularityId(String granularityId) {
        this.granularityId = granularityId;
    }

    public String getWindowDuration() {
        return windowDuration;
    }

    public void setWindowDuration(String windowDuration) {
        this.windowDuration = windowDuration;
    }

    public Integer getGranularityInTermsOfSeconds() {
        return granularityInTermsOfSeconds;
    }

    public void setGranularityInTermsOfSeconds(Integer granularityInTermsOfSeconds) {
        this.granularityInTermsOfSeconds = granularityInTermsOfSeconds;
    }

    public Integer getDisplayLimitInSeconds() {
        return displayLimitInSeconds;
    }

    public void setDisplayLimitInSeconds(Integer displayLimitInSeconds) {
        this.displayLimitInSeconds = displayLimitInSeconds;
    }

    public Integer getFetchIntervalAtOnceInSeconds() {
        return fetchIntervalAtOnceInSeconds;
    }

    public void setFetchIntervalAtOnceInSeconds(Integer fetchIntervalAtOnceInSeconds) {
        this.fetchIntervalAtOnceInSeconds = fetchIntervalAtOnceInSeconds;
    }

    public Integer getNumPartitionsForEachInterval() {
        return numPartitionsForEachInterval;
    }

    public void setNumPartitionsForEachInterval(Integer numPartitionsForEachInterval) {
        this.numPartitionsForEachInterval = numPartitionsForEachInterval;
    }

    public Integer getDisplayPriority() {
        return displayPriority;
    }

    public void setDisplayPriority(Integer displayPriority) {
        this.displayPriority = displayPriority;
    }

    @Override
    public String toString() {
        return "GranularityBean{" +
                "granularityId='" + granularityId + '\'' +
                ", windowDuration='" + windowDuration + '\'' +
                ", granularityInTermsOfSeconds=" + granularityInTermsOfSeconds +
                ", displayLimitInSeconds=" + displayLimitInSeconds +
                ", fetchIntervalAtOnceInSeconds=" + fetchIntervalAtOnceInSeconds +
                ", numPartitionsForEachInterval=" + numPartitionsForEachInterval +
                ", displayPriority=" + displayPriority +
                ", numParallelQuery=" + numParallelQuery +
                '}';
    }
}
