package in.nimbo.util;

public interface Observer<T> {
    void onStateChanged(T newState);
}
