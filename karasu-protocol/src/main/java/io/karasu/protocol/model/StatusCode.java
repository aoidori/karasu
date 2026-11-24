/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.protocol.model;

/**
 * Burrow-compatible 7-level consumer group / partition evaluation status codes.
 */
public enum StatusCode {
    NOTFOUND(0),
    OK(1),
    WARN(2),
    ERR(3),
    STOP(4),
    STALL(5),
    REWIND(6);

    private final int code;

    StatusCode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
