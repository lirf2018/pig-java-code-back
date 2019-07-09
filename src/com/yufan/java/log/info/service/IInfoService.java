package com.yufan.java.log.info.service;

/**
 * 创建人: lirf
 * 创建时间:  2019/4/12 9:21
 * 功能介绍:
 */
public interface IInfoService {

    /**
     * 保存接口业务结果数据(固定日期的,默认为前一天,或指定具体一天)
     * 接口访问次数和平均耗时/毫秒
     * @param date
     */
    void saveBusinessData(String date);

    /**
     * 保存接口业务结果数据(指定日期段)
     *
     * @param startDate
     * @param endDate
     */
    void saveBusinessData(String startDate, String endDate);



}
