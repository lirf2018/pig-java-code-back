package com.yufan.pig.chain;

import com.alibaba.fastjson.JSONObject;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * 创建人: lirf
 * 创建时间:  2018/12/14 17:19
 * 功能介绍:  筛选链去日志,保留需要的内容(获取接口请求业务类型)
 */
public class CleanChainLog extends EvalFunc<Tuple> {
    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        //获取输入字符串
        String strs = (String) input.get(0);
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
            tt.set(0, reqType);
            tt.set(1, date);
            return tt;
        }
        return null;
    }

//    public static void main(String[] args) throws Exception {
//        String strs = "2019-04-10 11:37:53,485 INFO [com.uhuibao.common.action.InfoAction] - 【1554867473485】接收参数:{\"req_type\":\"get_sys_params\",\"data\":{\"param_code\":\"jb\",\"param_type\":\"creditcard_post\"},\"locale\":\"CN\"}";
//        TupleFactory input = TupleFactory.getInstance();
//        Tuple tt = input.newTuple(1);
//        tt.set(0,strs);
//        CleanChainLog log = new CleanChainLog();
//        Tuple out = log.exec(tt);
//        System.out.println(out);
//    }
}
