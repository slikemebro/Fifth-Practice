package com.ua.glebkorobov.exception;

public class FileFindException extends RuntimeException{
    public FileFindException() {
    }

    public FileFindException(String message) {
        super(message);
    }

    public FileFindException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileFindException(Throwable cause) {
        super(cause);
    }

    public FileFindException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
