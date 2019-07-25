package in.nimbo.exception;

import java.io.IOException;

public class SiteDaoException extends Exception {
    public SiteDaoException(Exception e) {
        super(e);
    }
}
