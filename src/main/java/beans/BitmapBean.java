package beans;

import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import static managers.Utils.getTimeInSec;

public class BitmapBean {
    public Date startTime;
    public Date endTime;//currently of no use. even while saving to database not using this values
    public Map<GranularityBean, BitSet> granularityBeanBitSetMap = new HashMap<>();
    public boolean isDirty=false;

    public BitmapBean(Date startTime, Date endTime, Map<String, GranularityBean> granularityBeanMap) {
        this.startTime = startTime;
        this.endTime = endTime;
        for (GranularityBean granularityBean :
                granularityBeanMap.values()) {
            int bitsetSize = (int) ((getTimeInSec(endTime) - getTimeInSec(startTime)) / granularityBean.getGranularityInTermsOfSeconds());
            BitSet bitSet = new BitSet(bitsetSize);
            bitSet.clear();
            granularityBeanBitSetMap.put(granularityBean, bitSet);
        }
    }

}
