package com.mobiquityinc.spark.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

/**
 * Created by anand on 6/1/15.
 */
@Entity
@Table(name = "pv_spark", schema = "mobgateks@cassandra_pu")
public class PageVisitCount implements Serializable {

    @Id
    @Column(name = "ipaddress")
    private String ipAddress;

    @Column(name = "count")
    private Integer visitCount;

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public Integer getVisitCount() {
        return visitCount;
    }

    public void setVisitCount(Integer visitCount) {
        this.visitCount = visitCount;
    }
}
