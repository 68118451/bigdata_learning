package com.suishen.elasticsearch.core.query;

/**
 * Author: Alvin Li
 * Date: 11/08/2017
 * Time: 14:23
 */
public enum BulkOperationType {
    INDEX(1), UPDATE(2), DELETE(3);

    BulkOperationType(int op) {
        this.op = op;
        this.lowercase = this.toString().toLowerCase();
    }

    private int op;

    private String lowercase;

    public int getOp() {
        return op;
    }

    public String getLowercase() {
        return lowercase;
    }
}
