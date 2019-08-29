package in.nimbo.model;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Query {
    private String text;
    private String domain;
    private List<String> exclude;

    public Query(String text, String domain, List<String> exclude) {
        this.text = text;
        this.domain = domain;
        this.exclude = exclude;
    }

    public static Query build(String query) {
        String[] tokens = query.split(" ");
        String text = "";
        String domain = null;
        List<String> exclude = new LinkedList<>();
        Pattern domainPattern = Pattern.compile("^domain:(?<domain>\\S+)");
        Pattern excludePattern = Pattern.compile("^-(?<exclude>\\S+)");
        for (String token : tokens) {
            Matcher domainMatcher;
            Matcher excludeMatcher;
            domainMatcher = domainPattern.matcher(token);
            excludeMatcher = excludePattern.matcher(token);
            if (domainMatcher.find())
                domain = domainMatcher.group("domain");
            else if (excludeMatcher.find())
                exclude.add(excludeMatcher.group("exclude"));
            else
                text = text.concat(token + " ");
        }
        text = text.trim();
        System.out.println(text);
        return new Query(text, domain, exclude);
    }

    public String getText() {
        return text;
    }

    public String getDomain() {
        return domain;
    }

    public List<String> getExclude() {
        return exclude;
    }
}
