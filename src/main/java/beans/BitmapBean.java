package beans;

import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import static managers.Utils.getTimeInSec;

public class BitmapBean {
    public Date startTime;
    public Date endTime;//currently of no use. even while saving to database not using this values
    public HashMap<GranularityBean, BitSet> granularityBeanBitSetMap = new HashMap<>();
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

    public BitmapBean(BitmapBean bitmapBean) {
        this.startTime = new Date(bitmapBean.startTime.getTime());
        this.endTime = new Date(bitmapBean.endTime.getTime());
        granularityBeanBitSetMap = new HashMap<>();
        for(GranularityBean granularityBean: bitmapBean.granularityBeanBitSetMap.keySet()){
            granularityBeanBitSetMap.put(granularityBean, (BitSet) bitmapBean.granularityBeanBitSetMap.get(granularityBean).clone());
        }
        this.isDirty=false;
    }
}
