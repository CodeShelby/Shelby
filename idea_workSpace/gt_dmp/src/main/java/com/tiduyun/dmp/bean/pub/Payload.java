package com.tiduyun.dmp.bean.pub;

import com.tiduyun.dmp.bean.nc5.BD_ACCSUBJ_Before;
import com.tiduyun.dmp.bean.nc5.BD_ACCSUBJ_Data;

public class Payload {

    private String SCN;
    private String SEG_OWNER;
    private String  TABLE_NAME;
    private String  TIMESTAMP;
    private String  SQL_REDONFLAG1;
    private String  OPERATION;
    private BD_ACCSUBJ_Data data;
    private BD_ACCSUBJ_Before before;

    public BD_ACCSUBJ_Before getBefore() {
        return before;
    }

    public void setBefore(BD_ACCSUBJ_Before before) {
        this.before = before;
    }

    public String getSCN() {
        return SCN;
    }

    public void setSCN(String SCN) {
        this.SCN = SCN;
    }

    public String getSEG_OWNER() {
        return SEG_OWNER;
    }

    public void setSEG_OWNER(String SEG_OWNER) {
        this.SEG_OWNER = SEG_OWNER;
    }

    public String getTABLE_NAME() {
        return TABLE_NAME;
    }

    public void setTABLE_NAME(String TABLE_NAME) {
        this.TABLE_NAME = TABLE_NAME;
    }

    public String getTIMESTAMP() {
        return TIMESTAMP;
    }

    public void setTIMESTAMP(String TIMESTAMP) {
        this.TIMESTAMP = TIMESTAMP;
    }

    public String getSQL_REDONFLAG1() {
        return SQL_REDONFLAG1;
    }

    public void setSQL_REDONFLAG1(String SQL_REDONFLAG1) {
        this.SQL_REDONFLAG1 = SQL_REDONFLAG1;
    }

    public String getOPERATION() {
        return OPERATION;
    }

    public void setOPERATION(String OPERATION) {
        this.OPERATION = OPERATION;
    }

    public BD_ACCSUBJ_Data getData() {
        return data;
    }

    public void setData(BD_ACCSUBJ_Data data) {
        this.data = data;
    }
}
