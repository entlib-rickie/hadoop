package com.rickie.entity;

public class Article {
    private String id;
    private String title;
    private String from;
    private String times;
    private String readCounts;
    private String content;

    public Article() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Article(String id, String title, String from, String times, String readCounts, String content) {
        this.id = id;
        this.title = title;
        this.from = from;
        this.times = times;

        this.readCounts = readCounts;
        this.content = content;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTimes() {
        return times;
    }

    public void setTimes(String times) {
        this.times = times;
    }

    public String getReadCounts() {
        return readCounts;
    }

    public void setReadCounts(String readCounts) {
        this.readCounts = readCounts;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "Article{" +
                "id='" + id + '\'' +
                ", title='" + title + '\'' +
                ", from='" + from + '\'' +
                ", times='" + times + '\'' +
                ", readCounts='" + readCounts + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
