package utils;

import java.util.ArrayList;
import java.util.List;

import org.pac4j.scribe.builder.api.DropboxApi20;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;

import utils.DropBoxMsgs.*;

public class IODropbox {

    private static final String apiKey = "";
    private static final String apiSecret = "";
    private static final String accessTokenStr = "";

    private static final String API = "Dropbox-API-Arg";
    private static final String UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload";
    private static final String LIST_FOLDER_CONTINUE_URL = "https://api.dropboxapi.com/2/files/list_folder/continue";
    private static final String LIST_FOLDER_URL = "https://api.dropboxapi.com/2/files/list_folder";
    private static final String DOWNLOAD_URL = "https://content.dropboxapi.com/2/files/download";
    private static final String DELETE_URL = "https://api.dropboxapi.com/2/files/delete_v2";
    private static final String CREATE_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/create_folder_v2";

    private static final int HTTP_SUCCESS = 200;
    private static final int HTTP_CONFLICT = 409;
    
    private static final String CONTENT_TYPE_HDR = "Content-Type";
    private static final String JSON_CONTENT_TYPE_CHARSET = "application/json; charset=utf-8";
    private static final String JSON_CONTENT_TYPE_OCTET = "application/octet-stream";

    private static Gson json = null;
    private static OAuth20Service service = null;
    private static OAuth2AccessToken accessToken = null;

    public IODropbox() {
        json = new Gson();
        accessToken = new OAuth2AccessToken(accessTokenStr);
        service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
    }

    public void write(String filePath, byte[] bytes) {
        try {
            var writeFile = new OAuthRequest(Verb.POST, UPLOAD_URL);
            writeFile.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE_OCTET);
            writeFile.addHeader(API, json.toJson(new WriteFileArgs(filePath)));

            writeFile.setPayload(bytes);

            service.signRequest(accessToken, writeFile);

            Response r = service.execute(writeFile);
            if (r.getCode() != HTTP_SUCCESS) 
                throw new RuntimeException("Failed to write file: " + r.getCode() + " " + r.getBody());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte[] read(String filePath) {
        try{
            var readFile = new OAuthRequest(Verb.POST, DOWNLOAD_URL);
            readFile.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE_OCTET);
            readFile.addHeader(API, json.toJson(new ReadFileArgs(filePath)));
            
            service.signRequest(accessToken, readFile);
            
            Response r = service.execute(readFile);
            if (r.getCode() != HTTP_SUCCESS) 
                throw new RuntimeException("Failed to read file: " + r.getCode() + " " + r.getBody());
            return r.getBody().getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void delete(String filePath) {
        try {
            var deleteFile = new OAuthRequest(Verb.POST, DELETE_URL);
            deleteFile.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE_CHARSET);
            deleteFile.setPayload(json.toJson(new DeleteFileArgs(filePath)));

            service.signRequest(accessToken, deleteFile);

            Response r = service.execute(deleteFile);
            if (r.getCode() != HTTP_SUCCESS) 
                throw new RuntimeException("Failed to delete file: " + r.getCode() + " " + r.getBody());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void cleanDropbox() throws Exception{
        List<String> dir = listDirectory("/tukano");
        for (String d : dir) {
            delete("/tukano/"+ d);
        }
    }

    public List<String> listDirectory(String filePath) throws Exception{
        var directoryContents = new ArrayList<String>();

		var listDirectory = new OAuthRequest(Verb.POST, LIST_FOLDER_URL);
		listDirectory.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE_CHARSET);
		listDirectory.setPayload(json.toJson(new ListFolderArgs(filePath)));

		service.signRequest(accessToken, listDirectory);

		Response r = service.execute(listDirectory);;
		if (r.getCode() != HTTP_SUCCESS) 
			throw new RuntimeException(String.format("Failed to list directory: %s, Status: %d, \nReason: %s\n", filePath, r.getCode(), r.getBody()));

		var reply = json.fromJson(r.getBody(), ListFolderReturn.class);
		reply.getEntries().forEach( e -> directoryContents.add( e.toString() ) );
		
		while( reply.has_more() ) {
			listDirectory = new OAuthRequest(Verb.POST, LIST_FOLDER_CONTINUE_URL);
			listDirectory.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE_CHARSET);
			
			// In this case the arguments is just an object containing the cursor that was
			// returned in the previous reply.
			listDirectory.setPayload(json.toJson(new ListFolderContinueArgs(reply.getCursor())));
			service.signRequest(accessToken, listDirectory);

			r = service.execute(listDirectory);
			
			if (r.getCode() != HTTP_SUCCESS) 
				throw new RuntimeException(String.format("Failed to list directory: %s, Status: %d, \nReason: %s\n", filePath, r.getCode(), r.getBody()));
			
			reply = json.fromJson(r.getBody(), ListFolderReturn.class);
			reply.getEntries().forEach( e -> directoryContents.add( e.toString() ) );
		}
				
		return directoryContents;
    }

    public boolean createDirectory(String directoryName) throws Exception {
        var createFolder = new OAuthRequest(Verb.POST, CREATE_FOLDER_V2_URL);
		createFolder.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE_CHARSET);

		createFolder.setPayload(json.toJson(new CreateFolderV2Args(directoryName, false)));

		service.signRequest(accessToken, createFolder);
		
		Response r = service.execute(createFolder);

        return r.getCode() == HTTP_SUCCESS || r.getCode() == HTTP_CONFLICT;
    }
}
