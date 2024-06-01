package tukano.impl.grpc.servers;

import com.google.protobuf.ByteString;

import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import tukano.api.java.Blobs;
import tukano.impl.grpc.generated_java.BlobsGrpc;
import tukano.impl.grpc.generated_java.BlobsProtoBuf.*;
import tukano.impl.api.java.ExtendedBlobs;
import tukano.impl.java.servers.JavaBlobs;

public class GrpcBlobsServerStub extends AbstractGrpcStub implements BlobsGrpc.AsyncService {

	ExtendedBlobs impl = new JavaBlobs();

	@Override
	public ServerServiceDefinition bindService() {
		return BlobsGrpc.bindService(this);
	}

	@Override
	public void upload(UploadArgs request, StreamObserver<UploadResult> responseObserver) {
		String[] parts = request.getBlobId().split("\\?");
		String[] secondParts = parts[1].split("&");
		String timestamp = secondParts[0].replace("timestamp=", "");
        String verifier = secondParts[1].replace("verifier=", "");
		var res = impl.upload(parts[0], request.getData().toByteArray(), timestamp , verifier);
		if (!res.isOK())
			responseObserver.onError(errorCodeToStatus(res.error()));
		else {
			responseObserver.onNext(UploadResult.newBuilder().build());
			responseObserver.onCompleted();
		}
	}

	@Override
	public void download(DownloadArgs request, StreamObserver<DownloadResult> responseObserver) {
		var res = impl.downloadToSink(request.getBlobId(), (data) -> {
			responseObserver.onNext(DownloadResult.newBuilder().setChunk(ByteString.copyFrom(data)).build());
		});
		if (res.isOK())
			responseObserver.onCompleted();
		else
			responseObserver.onError(errorCodeToStatus(res.error()));
	}
	
	@Override
	public void delete(DeleteArgs request, StreamObserver<DeleteResult> responseObserver) {
		var res = impl.delete(request.getBlobId(), request.getToken());
		if (res.isOK()) {
			responseObserver.onNext(DeleteResult.newBuilder().build());
			responseObserver.onCompleted();
		}
		else
			responseObserver.onError(errorCodeToStatus(res.error()));

    }

	@Override
	public void deleteAllBlobs(DeleteAllBlobsArgs request, StreamObserver<DeleteAllBlobsResult> responseObserver) {
		var res = impl.deleteAllBlobs(request.getUserId(), request.getToken());
		if (res.isOK()) {
			responseObserver.onNext(DeleteAllBlobsResult.newBuilder().build());
			responseObserver.onCompleted();
		}
		else
			responseObserver.onError(errorCodeToStatus(res.error()));

    }
}
