package in.nimbo.exception;

import java.io.IOException;

public class SiteDaoException extends Exception {
    public SiteDaoException(IOException e) {
        super(e);
    }
}
