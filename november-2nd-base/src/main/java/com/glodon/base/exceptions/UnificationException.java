package com.glodon.base.exceptions;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.DriverManager;
import java.sql.SQLException;

public class UnificationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private UnificationException(Exception e) {
        super(e.getMessage(), e);
    }

    private UnificationException() {
    }

    private UnificationException(String message) {
        super(message);
    }

    private UnificationException(String message, Throwable cause) {
        super(message, cause);
    }

    private UnificationException(Throwable cause) {
        super(cause);
    }

    private UnificationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public static UnificationException get(String message, String p1) {
        return get(message, new String[]{p1});
    }

    public static UnificationException get(Throwable cause) {
        return new UnificationException(cause);
    }

    public static UnificationException getUnsupportedException(String message) {
        return get(message);
    }

    public static UnificationException get(String message, Throwable cause, String... params) {
        return new UnificationException(params != null && params.length > 0 ? String.format(message, params) : message, cause);
    }

    public static UnificationException get(String message, String... params) {
        return new UnificationException(params != null && params.length > 0 ? String.format(message, params) : message);
    }

    public static RuntimeException throwInternalError(String s) {
        throw getInternalError(s);
    }

    public static RuntimeException throwInternalError() {
        throw getInternalError();
    }

    public static RuntimeException getInternalError() {
        return getInternalError("Unexpected code path");
    }

    public static RuntimeException getInternalError(String s) {
        RuntimeException e = new RuntimeException(s);
        UnificationException.traceThrowable(e);
        return e;
    }

    public static UnificationException convert(Throwable e) {
        if (e instanceof UnificationException) {
            return (UnificationException) e;
        } else if (e instanceof SQLException) {
            return new UnificationException((SQLException) e);
        } else if (e instanceof InvocationTargetException) {
            return convertInvocation((InvocationTargetException) e, null);
        } else if (e instanceof IOException) {
            return get(e.toString(), e);
        } else if (e instanceof OutOfMemoryError) {
            return get(e);
        } else if (e instanceof StackOverflowError || e instanceof LinkageError) {
            return get(e.toString(), e);
        } else if (e instanceof Error) {
            throw (Error) e;
        }
        return get(e.toString(), e);
    }

    public static UnificationException convertInvocation(InvocationTargetException te, String message) {
        Throwable t = te.getTargetException();
        if (t instanceof SQLException || t instanceof UnificationException) {
            return convert(t);
        }
        message = message == null ? t.getMessage() : message + ": " + t.getMessage();
        return get(message, t);
    }

    public static UnificationException convertIOException(IOException e, String message) {
        if (message == null) {
            Throwable t = e.getCause();
            if (t instanceof UnificationException) {
                return (UnificationException) t;
            }
            return get(e.toString(), e);
        }
        return get(message, e);
    }

    public static IOException convertToIOException(Throwable e) {
        if (e instanceof IOException) {
            return (IOException) e;
        }
        return new IOException(e.toString(), e);
    }

    public static void traceThrowable(Throwable e) {
        PrintWriter writer = DriverManager.getLogWriter();
        if (writer != null) {
            e.printStackTrace(writer);
        }
    }
}
