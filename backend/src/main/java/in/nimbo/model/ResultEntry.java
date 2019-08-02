package in.nimbo.model;

public class ResultEntry {
    private String title;
    private String link;
    private String summary;

    public ResultEntry(String title, String link, String summary) {
        this.title = title;
        this.link = link;
        this.summary = summary;
    }

    public String getTitle() {
        return title;
    }

    public String getLink() {
        return link;
    }

    public String getSummary() {
        return summary;
    }
}

