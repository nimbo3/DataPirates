package in.nimbo.model.exceptions;

public class HbaseException extends Exception {
    public HbaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public HbaseException(String message) {
        super(message);
    }
}
