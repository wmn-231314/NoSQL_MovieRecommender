package com.service;

import java.io.Serializable;

public class TagInfo implements Serializable {
    private int tagid;
    private String name;
    private double relevance;

    public TagInfo(int tagid, String name, double relevance) {
        this.tagid = tagid;
        this.name = name;
        this.relevance = relevance;
    }

    public int getTagid() {
        return tagid;
    }

    public void setTagid(int tagid) {
        this.tagid = tagid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getRelevance() {
        return relevance;
    }

    public void setRelevance(double relevance) {
        this.relevance = relevance;
    }
}
