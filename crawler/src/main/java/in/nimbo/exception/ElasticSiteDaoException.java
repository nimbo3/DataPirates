package in.nimbo.exception;

public class ElasticSiteDaoException extends SiteDaoException{
    public ElasticSiteDaoException(Exception e) {
        super(e);
    }

    public ElasticSiteDaoException(String message, Exception cause) {
        super(message, cause);
    }

    public ElasticSiteDaoException(String message) {
        super(message);
    }
}
