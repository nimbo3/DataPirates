package in.nimbo.exception;

public class HbaseSiteDaoException extends SiteDaoException {
    public HbaseSiteDaoException(Exception e) {
        super(e);
    }

    public HbaseSiteDaoException(String message, Exception cause) {
        super(message, cause);
    }
}
