package com.service;

public class Profile {
    private int user_count;
    private double user_average;
    private double user_frequency;
    private double user_highrate;
    private String user_favor;

    private int movie_count;
    private double movie_average;
    private double movie_highrate;
    private int movie_release;
    private String movie_genre;

    public Profile(int user_count, double user_average, double user_frequency, double user_highrate,
                   String user_favor, int movie_count, double movie_average, double movie_highrate,
                   int movie_release, String movie_genre) {
        this.user_count = user_count;
        this.user_average = user_average;
        this.user_frequency = user_frequency;
        this.user_highrate = user_highrate;
        this.user_favor = user_favor;
        this.movie_count = movie_count;
        this.movie_average = movie_average;
        this.movie_highrate = movie_highrate;
        this.movie_release = movie_release;
        this.movie_genre = movie_genre;
    }

    public Profile(int user_count, double user_average, double user_frequency, double user_highrate, String user_favor) {
        this.user_count = user_count;
        this.user_average = user_average;
        this.user_frequency = user_frequency;
        this.user_highrate = user_highrate;
        this.user_favor = user_favor;
    }

    public void setMovieProfile(int movie_count, double movie_average, double movie_highrate,
                                int movie_release, String movie_genre){
        this.movie_count = movie_count;
        this.movie_average = movie_average;
        this.movie_highrate = movie_highrate;
        this.movie_release = movie_release;
        this.movie_genre = movie_genre;
    }

    public int getUser_count() {
        return user_count;
    }

    public double getUser_average() {
        return user_average;
    }

    public double getUser_frequency() {
        return user_frequency;
    }

    public double getUser_highrate() {
        return user_highrate;
    }

    public String getUser_favor() {
        return user_favor;
    }

    public int getMovie_count() {
        return movie_count;
    }

    public double getMovie_average() {
        return movie_average;
    }

    public double getMovie_highrate() {
        return movie_highrate;
    }

    public int getMovie_release() {
        return movie_release;
    }

    public String getMovie_genre() {
        return movie_genre;
    }
}
