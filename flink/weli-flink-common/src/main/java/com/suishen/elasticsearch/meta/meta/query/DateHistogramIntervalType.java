package com.suishen.elasticsearch.meta.meta.query;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

/**
 * DateHistogramInterval 中表示时间间隔的单位
 *
 * Author: Alvin Li
 * Date: 7/13/16
 * Time: 15:02
 */
public enum DateHistogramIntervalType {
    DAY(DateHistogramInterval.DAY,"d"),
    HOUR(DateHistogramInterval.HOUR,"h"),
    MINUTE(DateHistogramInterval.MINUTE,"m"),
    MONTH(DateHistogramInterval.MONTH,"M"),
    QUARTER(DateHistogramInterval.QUARTER,"q"),
    SECOND(DateHistogramInterval.SECOND,"s"),
    WEEK(DateHistogramInterval.WEEK,"w"),
    YEAR(DateHistogramInterval.YEAR,"y")
    ;

    private DateHistogramInterval type;
    private String unit;

    DateHistogramIntervalType(DateHistogramInterval type, String unit) {
        this.type = type;
        this.unit = unit;
    }

    public DateHistogramInterval getType() {
        return type;
    }

    public String getUnit() {
        return unit;
    }



}
