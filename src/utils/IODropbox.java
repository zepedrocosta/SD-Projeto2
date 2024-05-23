package utils;

import java.io.File;

import org.pac4j.scribe.builder.api.DropboxApi20;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;

public class IODropbox {
    
    private static final String apiKey = "e1cnnhudp3nwwu5";
	private static final String apiSecret = "mji6dkyofw78cgm";
	private static final String accessTokenStr = "sl.B1xv4VwGE9yGXnRWI9Uu2AoJmoJryfzZYRLWUesU8ubzeuMpkYh5yOiJzEpZRrk9m19W0g2FQqlJMfKpEgeo8ghEHDvtcRwL3FHcBJB9C-aEP8kPNGHLSXmGyLlNuRB0DW7PfPsN5pUE";
	
    private static final String UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload";
    private static final String LIST_FOLDER_CONTINUE_URL = "https://api.dropboxapi.com/2/files/list_folder/continue";
    private static final String LIST_FOLDER_URL = "https://api.dropboxapi.com/2/files/list_folder";
    private static final String DOWNLOAD_URL = "https://content.dropboxapi.com/2/files/download";
    private static final String DELETE_URL = "https://api.dropboxapi.com/2/files/delete_v2";
    private static final String CREATE_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/create_folder_v2";
    
    private static final int HTTP_SUCCESS = 200;
	private static final String CONTENT_TYPE_HDR = "Content-Type";
	private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";

    private final Gson json;
	private final OAuth20Service service;
	private final OAuth2AccessToken accessToken;

    public IODropbox() {
        json = new Gson();
		accessToken = new OAuth2AccessToken(accessTokenStr);
		service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
    }

    public static byte[] read(File file) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'read'");
    }

    public static void write(File file, byte[] bytes) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'write'");
    }

    public static void delete(File file) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'delete'");
    }

    public static void cleanDropbox() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'cleanDropbox'");
    }
}
