package com.yufan.pig.info;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 创建人: lirf
 * 创建时间:  2019/4/10 14:43
 * 功能介绍: 处理接口响应时间
 */
public class InfoResponseTime extends EvalFunc<Tuple> {

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
        //匹配时间
        String patternTime = "执行耗时:【(.*?)】毫秒";
        Pattern rTime = Pattern.compile(patternTime);
        Matcher mTime = rTime.matcher(strs);
        String time = null;
        if (mTime.find()) {
            time = mTime.group(1);
        }
        if (StringUtils.isEmpty(time) ) {
            return null;
        }
        //匹配业务
        String patternType = "- type=\\u005b(.*?)\\u005d【";
        Pattern rType = Pattern.compile(patternType);
        Matcher mType = rType.matcher(strs);
        String type = null;
        if (mType.find()) {
            type = mType.group(1);
        }
        if (StringUtils.isEmpty(type) ) {
            return null;
        }
        String[] words = strs.split(" ");
        if (words.length > 1) {
            String date = words[0];//获取日期
            TupleFactory newTuple = TupleFactory.getInstance();
            Tuple tt = newTuple.newTuple(3);
            tt.set(0, date);
            tt.set(1, type);
            tt.set(2, Integer.parseInt(time));
            return tt;
        }
        return null;
    }

    /*public static void main(String[] args) throws Exception {
        String strs = "2019-04-10 14:57:01,235 INFO [com.uhuibao.common.action.InfoAction] - type=[get_user_credit_card_detail]【1554879420970】执行耗时:【265】毫秒";
        TupleFactory input = TupleFactory.getInstance();
        Tuple tt = input.newTuple(1);
        tt.set(0, strs);
        InfoResponseTime log = new InfoResponseTime();
        Tuple out = log.exec(tt);
        System.out.println(out);
    }*/
}
