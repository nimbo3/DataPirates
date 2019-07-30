package in.nimbo.exception;

public class SiteDaoException extends Exception {
    public SiteDaoException(Exception e) {
        super(e);
    }

    public SiteDaoException(String message, Exception cause) {
        super(message, cause);
    }
}
