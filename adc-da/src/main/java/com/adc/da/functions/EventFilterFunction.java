package com.adc.da.functions;

import com.adc.da.bean.OdsData;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;

public class EventFilterFunction extends RichFilterFunction<OdsData[]> {
    public ValueState<OdsData[]> state = null;
    public OdsData[] value = null;

   /* @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<OdsData[]>("static", OdsData[].class));
    }*/

    @Override
    public void close() throws Exception {
        state.clear();
    }

    @Override
    public boolean filter(OdsData[] odsData) throws Exception {
        // 获取状态值
        value = state.value();
        if (value == null || value[1].getMsgTime() != odsData[1].getMsgTime()) {
            state.update(odsData);
            return true;
        }
        return false;
    }
}