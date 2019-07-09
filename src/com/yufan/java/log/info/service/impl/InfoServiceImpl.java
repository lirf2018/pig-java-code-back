package com.yufan.java.log.info.service.impl;

import com.yufan.java.common.dao.IInfoJpaDao;
import com.yufan.java.log.info.service.IInfoService;
import com.yufan.java.pojo.TbVisitBusiness;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * 创建人: lirf
 * 创建时间:  2019/4/12 9:21
 * 功能介绍:
 */
public class InfoServiceImpl implements IInfoService {

    private Logger LOG = Logger.getLogger(InfoServiceImpl.class);

    private IInfoJpaDao infoJpaDao;

    @Override
    public void saveBusinessData(String date) {
        List<TbVisitBusiness> business = infoJpaDao.findAll();

        System.out.println("------------->business="+business.size());
    }

    @Override
    public void saveBusinessData(String startDate, String endDate) {

    }

}
