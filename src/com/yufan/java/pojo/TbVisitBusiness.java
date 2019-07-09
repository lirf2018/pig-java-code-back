package com.yufan.java.pojo;

import javax.persistence.*;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * 创建人: lirf
 * 创建时间:  2019/4/12 10:29
 * 功能介绍:
 */
@Entity
@Table(name = "tb_visit_business", catalog = "")
public class TbVisitBusiness {
    private int id;
    private String businessCode;
    private String sysCode;
    private Timestamp visitDate;
    private int visitCount;
    private BigDecimal responseTime;

    @Id
    @Column(name = "id")
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Basic
    @Column(name = "business_code")
    public String getBusinessCode() {
        return businessCode;
    }

    public void setBusinessCode(String businessCode) {
        this.businessCode = businessCode;
    }

    @Basic
    @Column(name = "sys_code")
    public String getSysCode() {
        return sysCode;
    }

    public void setSysCode(String sysCode) {
        this.sysCode = sysCode;
    }

    @Basic
    @Column(name = "visit_date")
    public Timestamp getVisitDate() {
        return visitDate;
    }

    public void setVisitDate(Timestamp visitDate) {
        this.visitDate = visitDate;
    }

    @Basic
    @Column(name = "visit_count")
    public int getVisitCount() {
        return visitCount;
    }

    public void setVisitCount(int visitCount) {
        this.visitCount = visitCount;
    }

    @Basic
    @Column(name = "response_time")
    public BigDecimal getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(BigDecimal responseTime) {
        this.responseTime = responseTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TbVisitBusiness that = (TbVisitBusiness) o;
        return id == that.id &&
                visitCount == that.visitCount &&
                Objects.equals(businessCode, that.businessCode) &&
                Objects.equals(sysCode, that.sysCode) &&
                Objects.equals(visitDate, that.visitDate) &&
                Objects.equals(responseTime, that.responseTime);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, businessCode, sysCode, visitDate, visitCount, responseTime);
    }
}
