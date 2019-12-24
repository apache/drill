/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.rest;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;

public abstract class HttpStatus {

    private static final Map<Integer, String> CODE_TO_TEXT = new HashMap<Integer, String>();

    public static String getText(int code) {
        if (code < 100 || code / 100 > 5) {
            throw new EsHadoopIllegalArgumentException("Invalid http code");
        }

        return CODE_TO_TEXT.get(Integer.valueOf(code));
    }

    private static void addCode(int code, String text) {
        CODE_TO_TEXT.put(Integer.valueOf(code), text);
    }

    public static final int CONTINUE = 100;
    public static final int SWITCHING_PROTOCOLS = 101;
    public static final int PROCESSING = 102;

    public static final int OK = 200;
    public static final int CREATED = 201;
    public static final int ACCEPTED = 202;
    public static final int NON_AUTHORITATIVE_INFORMATION = 203;
    public static final int NO_CONTENT = 204;
    public static final int RESET_CONTENT = 205;
    public static final int PARTIAL_CONTENT = 206;
    public static final int MULTI_STATUS = 207;
    public static final int ALREADY_REPORTED = 208;
    public static final int IM_USED = 226;

    public static final int MULTIPLE_CHOICES = 300;
    public static final int MOVED_PERMANENTLY = 301;
    public static final int MOVED_TEMPORARILY = 302;
    public static final int SEE_OTHER = 303;
    public static final int NOT_MODIFIED = 304;
    public static final int USE_PROXY = 305;
    public static final int SWITCH_PROXY = 306;
    public static final int TEMPORARY_REDIRECT = 307;
    public static final int PERMANENT_REDIRECT = 308;

    public static final int BAD_REQUEST = 400;
    public static final int UNAUTHORIZED = 401;
    public static final int PAYMENT_REQUIRED = 402;
    public static final int FORBIDDEN = 403;
    public static final int NOT_FOUND = 404;
    public static final int METHOD_NOT_ALLOWED = 405;
    public static final int NOT_ACCEPTABLE = 406;
    public static final int PROXY_AUTHENTICATION_REQUIRED = 407;
    public static final int REQUEST_TIMEOUT = 408;
    public static final int CONFLICT = 409;
    public static final int GONE = 410;
    public static final int LENGTH_REQUIRED = 411;
    public static final int PRECONDITION_FAILED = 412;
    public static final int REQUEST_ENTITY_TOO_LARGE = 413;
    public static final int REQUEST_URI_TOO_LONG = 414;
    public static final int UNSUPPORTED_MEDIA_TYPE = 415;
    public static final int REQUESTED_RANGE_NOT_SATISFIABLE = 416;
    public static final int EXPECTATION_FAILED = 417;
    public static final int I_M_A_TEAPOT = 418;
    public static final int AUTHENTICATION_TIMEOUT = 419;
    public static final int UNPROCESSABLE_ENTITY = 422;
    public static final int LOCKED = 423;
    public static final int METHOD_FAILURE = 424;
    public static final int UNORDERED_COLLECTION = 425;
    public static final int UPGRADE_REQUIRED = 426;
    public static final int PRECONDITION_REQUIRED = 428;
    public static final int TOO_MANY_REQUESTS = 429;
    public static final int REQUEST_HEADER_FIELDS_TOO_LARGE = 431;

    public static final int INTERNAL_SERVER_ERROR = 500;
    public static final int NOT_IMPLEMENTED = 501;
    public static final int BAD_GATEWAY = 502;
    public static final int SERVICE_UNAVAILABLE = 503;
    public static final int GATEWAY_TIMEOUT = 50;
    public static final int HTTP_VERSION_NOT_SUPPORTED = 505;
    public static final int VARIANT_ALSO_NEGOTIATES = 506;
    public static final int INSUFFICIENT_STORAGE = 507;
    public static final int LOOP_DETECTED = 508;
    public static final int BANDWIDTH_LIMIT_EXCEEDED = 509;
    public static final int NOT_EXTENDED = 510;
    public static final int NETWORK_AUTHENTICATION_REQUIRED = 511;
    public static final int ORIGIN_ERROR = 520;
    public static final int CONNECTION_TIMED_OUT = 522;
    public static final int PROXY_DECLINED_REQUEST = 523;
    public static final int A_TIMEOUT_OCCURRED = 524;
    public static final int NETWORK_READ_TIMEOUT_ERROR = 598;
    public static final int NETWORK_CONNECT_TIMEOUT_ERROR = 599;

    static {
        addCode(CONTINUE, "Continue");
        addCode(SWITCHING_PROTOCOLS, "Switching Protocols");
        addCode(PROCESSING, "Processing");

        addCode(OK, "OK");
        addCode(CREATED, "Created");
        addCode(ACCEPTED, "Accepted");
        addCode(NON_AUTHORITATIVE_INFORMATION, "Non Authoritative Information");
        addCode(NO_CONTENT, "No Content");
        addCode(RESET_CONTENT, "Reset Content");
        addCode(PARTIAL_CONTENT, "Partial Content");
        addCode(MULTI_STATUS, "Multi-Status");
        addCode(ALREADY_REPORTED, "Already Reported");
        addCode(IM_USED, "IM Used");

        addCode(MULTIPLE_CHOICES, "Multiple Choices");
        addCode(MOVED_PERMANENTLY, "Moved Permanently");
        addCode(MOVED_TEMPORARILY, "Moved Temporarily");
        addCode(SEE_OTHER, "See Other");
        addCode(NOT_MODIFIED, "Not Modified");
        addCode(USE_PROXY, "Use Proxy");
        addCode(SWITCH_PROXY, "Switch Proxy");
        addCode(TEMPORARY_REDIRECT, "Temporary Redirect");
        addCode(PERMANENT_REDIRECT, "Permanent Redirect");

        addCode(BAD_REQUEST, "Bad Request");
        addCode(UNAUTHORIZED, "Unauthorized");
        addCode(PAYMENT_REQUIRED, "Payment Required");
        addCode(FORBIDDEN, "Forbidden");
        addCode(NOT_FOUND, "Not Found");
        addCode(METHOD_NOT_ALLOWED, "Method Not Allowed");
        addCode(NOT_ACCEPTABLE, "Not Acceptable");
        addCode(PROXY_AUTHENTICATION_REQUIRED, "Proxy Authentication Required");
        addCode(REQUEST_TIMEOUT, "Request Timeout");
        addCode(CONFLICT, "Conflict");
        addCode(GONE, "Gone");
        addCode(LENGTH_REQUIRED, "Length Required");
        addCode(PRECONDITION_FAILED, "Precondition Failed");
        addCode(REQUEST_ENTITY_TOO_LARGE, "Request Entity Too Large");
        addCode(REQUEST_URI_TOO_LONG, "Request-URI Too Long");
        addCode(UNSUPPORTED_MEDIA_TYPE, "Unsupported Media Type");
        addCode(REQUESTED_RANGE_NOT_SATISFIABLE, "Requested Range Not Satisfiable");
        addCode(EXPECTATION_FAILED, "Expectation Failed");
        addCode(I_M_A_TEAPOT, "I'm a teapot");
        addCode(AUTHENTICATION_TIMEOUT, "Authentication Timeout");
        addCode(UNPROCESSABLE_ENTITY, "Unprocessable Entity");
        addCode(LOCKED, "Locked");
        addCode(METHOD_FAILURE, "Method Failure");
        addCode(UNORDERED_COLLECTION, "Unordered Collection");
        addCode(UPGRADE_REQUIRED, "Upgrade Required");
        addCode(PRECONDITION_REQUIRED, "Precondition Required");
        addCode(TOO_MANY_REQUESTS, "Too Many Requests");
        addCode(REQUEST_HEADER_FIELDS_TOO_LARGE, "Request Header Fields Too Large");

        addCode(INTERNAL_SERVER_ERROR, "Internal Server Error");
        addCode(NOT_IMPLEMENTED, "Not Implemented");
        addCode(BAD_GATEWAY, "Bad Gateway");
        addCode(SERVICE_UNAVAILABLE, "Service Unavailable");
        addCode(GATEWAY_TIMEOUT, "Gateway Timeout");
        addCode(HTTP_VERSION_NOT_SUPPORTED, "Http Version Not Supported");
        addCode(VARIANT_ALSO_NEGOTIATES, "Variant Also Negociates");
        addCode(INSUFFICIENT_STORAGE, "Insufficient Storage");
        addCode(LOOP_DETECTED, "Loop Detected");
        addCode(BANDWIDTH_LIMIT_EXCEEDED, "Bandwidth Limit Exceeded");
        addCode(NOT_EXTENDED, "Not Extended");
        addCode(NETWORK_AUTHENTICATION_REQUIRED, "Network Authentication Required");
        addCode(ORIGIN_ERROR, "Origin Error");
        addCode(CONNECTION_TIMED_OUT, "Connection timed out");
        addCode(PROXY_DECLINED_REQUEST, "Proxy Declined Request");
        addCode(A_TIMEOUT_OCCURRED, "A timeout occurred");
        addCode(NETWORK_READ_TIMEOUT_ERROR, "Network read timeout error");
        addCode(NETWORK_CONNECT_TIMEOUT_ERROR, "Network connect timeout error");
    }

    public static boolean canRetry(int status) {
        return SERVICE_UNAVAILABLE == status || TOO_MANY_REQUESTS == status;
    }

    public static boolean isServerError(int status) {
        return status / 100 == 5;
    }

    public static boolean isClientError(int status) {
        return status / 100 == 4;
    }

    public static boolean isRedirection(int status) {
        return status / 100 == 3;
    }

    public static boolean isSuccess(int status) {
        return status / 100 == 2;
    }

    public static boolean isInformal(int status) {
        return status / 100 == 1;
    }
}