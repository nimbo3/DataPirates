package in.nimbo.util;

import java.util.ArrayList;

public class Observable<T> {
    private T state;

    private ArrayList<Observer<? super T>> observers = new ArrayList<>();

    public Observable(T state) {
        this.state = state;
    }

    public void addObserver(Observer<? super T> observer) {
        observers.add(observer);
        notifyObserver(observer);
    }

    public void removeObserver(Observer<? super T> observer) {
        observers.remove(observer);
    }

    public T getState() {
        return state;
    }

    public synchronized void setState(T state) {
        this.state = state;
        notifyAllObservers();
    }

    private void notifyObserver(Observer<? super T> observer) {
        observer.onStateChanged(getState());
    }

    private void notifyAllObservers() {
        observers.forEach(observer -> observer.onStateChanged(getState()));
    }
}