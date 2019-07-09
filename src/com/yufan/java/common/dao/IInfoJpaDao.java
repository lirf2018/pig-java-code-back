package com.yufan.java.common.dao;

import com.yufan.java.pojo.TbVisitBusiness;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

/**
 * 创建人: lirf
 * 创建时间:  2019/4/12 18:01
 * 功能介绍:
 */
public interface IInfoJpaDao extends JpaRepository<TbVisitBusiness,Integer> ,JpaSpecificationExecutor<TbVisitBusiness> {
}
