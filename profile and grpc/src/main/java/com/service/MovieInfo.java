package com.service;

import redis.clients.jedis.Tuple;

import javax.swing.text.html.HTMLDocument;
import java.io.Serializable;
import java.util.*;

public class MovieInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    private String title;
    private long releaseTime;
    private List<String> genre;
    private Set<TagInfo> topTags;

    public MovieInfo(String title,long releaseTime) {
        this.title = title;
        this.releaseTime = releaseTime;
        genre = new ArrayList();
        topTags = null;
    }

    public void setGenre(String[] info) {
        int len = info.length;
        for (int i = 0; i < len; i++) {
            genre.add(info[i]);
//            System.out.println(info[i]);
        }
    }

    public Set<TagInfo> getTopTags() {
        return topTags;
    }

    public void setTopTags(Set<TagInfo> tagInfo) {
        topTags = tagInfo;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getGenre() {
        return genre;
    }

    public long getReleaseTime() {
        return releaseTime;
    }

    public void setReleaseTime(long releaseTime) {
        this.releaseTime = releaseTime;
    }
}
