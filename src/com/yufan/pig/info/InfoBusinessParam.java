package com.yufan.pig.info;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * 创建人: lirf
 * 创建时间:  2019/4/10 14:42
 * 功能介绍: 处理info接口接收处理业务
 */
public class InfoBusinessParam extends EvalFunc<Tuple> {

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        //获取输入字符串
        String strs = (String) input.get(0);
        if (StringUtils.isEmpty(strs)) {
            return null;
        }
        String[] businessArray = strs.split("接收参数:");
        if (businessArray.length > 1) {
            JSONObject result = JSONObject.parseObject(businessArray[1]);
            String reqType = result.getString("req_type");//获取业务类型
            if (null == reqType) {
                return null;
            }
            String[] dateArray = businessArray[0].split(" ");//获取日期
            String date = dateArray[0];
            TupleFactory newTuple = TupleFactory.getInstance();
            Tuple tt = newTuple.newTuple(2);
            tt.set(0, date);
            tt.set(1, reqType);
            return tt;
        }
        return null;
    }

//    public static void main(String[] args) throws Exception {
//        String strs = "2019-04-10 11:37:53,475 INFO [com.uhuibao.common.action.InfoAction] - 【1554867473475】接收参数:{\"req_type\":\"card_main_info\",\"data\":{\"channel_code\":\"jb\"},\"locale\":\"CN\"}";
//        TupleFactory input = TupleFactory.getInstance();
//        Tuple tt = input.newTuple(1);
//        tt.set(0, strs);
//        InfoBusinessParam log = new InfoBusinessParam();
//        Tuple out = log.exec(tt);
//        System.out.println(out);
//    }
}
