package tukano.impl.rest.servers;

import jakarta.inject.Singleton;
import tukano.impl.api.java.ExtendedBlobs;
import tukano.impl.api.rest.RestExtendedBlobs;
import tukano.impl.java.servers.JavaBlobsDB;

@Singleton
public class RestBlobsDBResource extends RestResource implements RestExtendedBlobs {

	final ExtendedBlobs impl;

	public RestBlobsDBResource() {
		this.impl = new JavaBlobsDB();
	}

	@Override
	public void upload(String blobId, byte[] bytes, String timestamp, String token) {
		super.resultOrThrow( impl.upload(blobId, bytes, timestamp, token));
	}

	@Override
	public byte[] download(String blobId, String timestamp, String token) {
		return super.resultOrThrow( impl.download( blobId , timestamp, token ));
	}

	@Override
	public void delete(String blobId, String token) {
		super.resultOrThrow(impl.delete(blobId, token));
	}

	@Override
	public void deleteAllBlobs(String userId, String password) {
		super.resultOrThrow(impl.deleteAllBlobs(userId, password));
	}
}
