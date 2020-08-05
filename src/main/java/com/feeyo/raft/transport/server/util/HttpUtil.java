package com.feeyo.raft.transport.server.util;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import com.feeyo.net.codec.http.HttpHeaderNames;
import com.feeyo.net.codec.http.HttpResponse;
import com.feeyo.net.codec.http.HttpResponseEncoder;
import com.feeyo.net.codec.http.HttpResponseStatus;
import com.feeyo.raft.transport.server.HttpConnection;


public class HttpUtil {
	///
	private static HttpResponseEncoder httpResponseEncoder = new HttpResponseEncoder();
	//
    public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    public static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
    private static final String HTTP_MIME_TYPE__VIDEO_MP2T = "video/MP2T";
    private static final String HTTP_MIME_TYPE__TEXT = "text/html";
    private static final String HTTP_MIME_TYPE__MPEG_URL_APPLE = "application/vnd.apple.mpegurl";
    private static final String HTTP_11 = "HTTP/1.1";
    //
    private static Map<String, String> DEFAULT_MIME_TYPES;
    static {
        Map<String, String> mimeTypes = new HashMap<String, String>();
        mimeTypes.put("mp3",HTTP_MIME_TYPE__VIDEO_MP2T);
        mimeTypes.put("ts",HTTP_MIME_TYPE__VIDEO_MP2T);
        mimeTypes.put("m3u8",HTTP_MIME_TYPE__MPEG_URL_APPLE);
        mimeTypes.put("txt", "text/plain");
        mimeTypes.put("css", "text/css");
        mimeTypes.put("csv", "text/csv");
        mimeTypes.put("htm", "text/html");
        mimeTypes.put("html", "text/html");
        mimeTypes.put("js", "application/javascript");
        mimeTypes.put("xhtml", "application/xhtml+xml");
        mimeTypes.put("json", "application/json");
        mimeTypes.put("pdf", "application/pdf");
        mimeTypes.put("zip", "application/zip");
        mimeTypes.put("tar", "application/x-tar");
        mimeTypes.put("gif", "image/gif");
        mimeTypes.put("jpeg", "image/jpeg");
        mimeTypes.put("jpg", "image/jpg");
        mimeTypes.put("tiff", "image/tiff");
        mimeTypes.put("tif", "image/tif");
        mimeTypes.put("png", "image/png");
        mimeTypes.put("svg", "image/svg+xml");
        mimeTypes.put("ico", "image/vnd.microsoft.icon");
        DEFAULT_MIME_TYPES = Collections.unmodifiableMap(mimeTypes);
    }
    
    //
    private static ThreadLocal<SimpleDateFormat> dateFormatter = new ThreadLocal<SimpleDateFormat>() {
    	@Override
		protected SimpleDateFormat initialValue() {
    		return new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
    	}
    };
    
    
	private static final HttpResponse redirect(HttpResponseStatus status, String url) {
		HttpResponse response = new HttpResponse(HTTP_11, status.getCode(), status.getReasonPhrase());
		response.addHeader(HttpHeaderNames.LOCATION, url);
		response.addHeader(HttpHeaderNames.CONTENT_LENGTH, "0");
		return response;
	}
    
    public static final HttpResponse redirectFound(String url){
        return redirect(HttpResponseStatus.FOUND, url);
    }
    
    public static final HttpResponse redirectTemporarily(String url){
        return redirect(HttpResponseStatus.TEMPORARY_REDIRECT, url);
    }

    public static final HttpResponse redirectPermanently(String url){
        return redirect(HttpResponseStatus.MOVED_PERMANENTLY, url);
    }
    ///
    //  send 
    public static void sendOk(HttpConnection conn) {
    	sendOk(conn, null);
	}
	
	public static void sendOk(HttpConnection conn, byte[] content) {
		send(conn, HttpResponseStatus.OK, content);
	}

	public static void sendError(HttpConnection conn, String reason) {
		send(conn, HttpResponseStatus.INTERNAL_SERVER_ERROR);
	}
    
    public static void sendNotModified(HttpConnection conn) {
    	send(conn, HttpResponseStatus.NOT_MODIFIED);
    }
    
    public static void send(HttpConnection conn, HttpResponseStatus status) {
    	send(conn, status, null);
    }
    
    public static void send(HttpConnection conn, HttpResponseStatus status, byte[] content) {
    	if ( conn == null )
			return;
    	//
        HttpResponse response = new HttpResponse(HTTP_11, status.getCode(), status.getReasonPhrase());
        response.setContent( content );
        //
        ByteBuffer buffer = httpResponseEncoder.encodeToByteBuffer( response );
        conn.write( buffer );
    }

    public static String getMimeType (String filename) {
        int lastDot = filename.lastIndexOf('.');
        if (lastDot == -1) 
            return HTTP_MIME_TYPE__TEXT;
        //
        String mimeType = DEFAULT_MIME_TYPES.get(filename.substring(lastDot+1).toLowerCase());
        if (mimeType == null)
            return  HTTP_MIME_TYPE__TEXT;
        else
            return mimeType;
    }

    public static String getDateString(long timemillis) {
    	SimpleDateFormat sdf = dateFormatter.get();
    	sdf.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));
        return sdf.format(new Date(timemillis));
    }
}