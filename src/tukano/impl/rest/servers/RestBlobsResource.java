package tukano.impl.rest.servers;

import jakarta.inject.Singleton;
import tukano.impl.api.java.ExtendedBlobs;
import tukano.impl.api.rest.RestExtendedBlobs;
import tukano.impl.java.servers.JavaBlobs;

@Singleton
public class RestBlobsResource extends RestResource implements RestExtendedBlobs {

	final ExtendedBlobs impl;
	
	public RestBlobsResource() {
		this.impl = new JavaBlobs();
	}
	
	@Override
	public void upload(String blobId, byte[] bytes, String timestamp, String verifier) {
		super.resultOrThrow( impl.upload(blobId, bytes, timestamp, verifier));
	}

	@Override
	public byte[] download(String blobId, String timestamp, String verifier) {
		return super.resultOrThrow( impl.download( blobId , timestamp, verifier ));
	}

	@Override
	public void delete(String blobId, String token) {
		super.resultOrThrow( impl.delete( blobId, token ));
	}
	
	@Override
	public void deleteAllBlobs(String userId, String password) {
		super.resultOrThrow( impl.deleteAllBlobs( userId, password ));
	}
}
