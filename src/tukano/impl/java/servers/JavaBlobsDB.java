package tukano.impl.java.servers;

import static java.lang.String.format;
import static tukano.api.java.Result.error;
import static tukano.api.java.Result.ok;
import static tukano.api.java.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.java.Result.ErrorCode.CONFLICT;
import static tukano.api.java.Result.ErrorCode.FORBIDDEN;
import static tukano.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.java.Result.ErrorCode.NOT_FOUND;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

import tukano.api.java.Result;
import tukano.impl.api.java.ExtendedBlobs;
import tukano.impl.java.clients.Clients;
import utils.Hash;
import utils.Hex;
import utils.IODropbox;
import utils.IP;
import utils.Token;


public class JavaBlobsDB implements ExtendedBlobs {
	private static final String BLOBS_ROOT_DIR = "/tukano/" + IP.hostname() + "/";

	private static Logger Log = Logger.getLogger(JavaBlobsDB.class.getName());

	private static final int CHUNK_SIZE = 4096;

	@Override
	public Result<Void> upload(String blobId, byte[] bytes) {
		Log.info(() -> format("upload : blobId = %s, sha256 = %s\n", blobId, Hex.of(Hash.sha256(bytes))));

		if (!validBlobId(blobId))
			return error(FORBIDDEN);

		String filePath = toFilePath(blobId);
		IODropbox dropbox = new IODropbox();

		if (filePath == null)
			return error(BAD_REQUEST);

		try {
			if (!dropbox.createDirectory(blobFolder(filePath)))
				return error(INTERNAL_ERROR);
			if (dropbox.listDirectory(blobFolder(filePath)).contains(filePath)) {
				byte[] file = IODropbox.read(filePath);
				if (Arrays.equals(Hash.sha256(bytes), Hash.sha256(file)))
					return ok();
				else
					return error(CONFLICT);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		dropbox.write(filePath, bytes);
		return ok();

	}

	@Override
	public Result<byte[]> download(String blobId) {
		Log.info(() -> format("download : blobId = %s\n", blobId));

		String filePath = toFilePath(blobId);
		if (filePath == null)
			return error(BAD_REQUEST);

		IODropbox dropbox = new IODropbox();
		try {
			if (hasBlob(filePath, dropbox)) {
				byte[] file = IODropbox.read(filePath);	
				return ok(file);
			} else
				return error(NOT_FOUND);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Result<Void> downloadToSink(String blobId, Consumer<byte[]> sink) {
		Log.info(() -> format("downloadToSink : blobId = %s\n", blobId));

		var filePath = toFilePath(blobId);
		IODropbox dropbox = new IODropbox();

		if (filePath == null)
			return error(BAD_REQUEST);

		try {
			if (dropbox.listDirectory(blobFolder(filePath)).contains(filePath))
				return error(NOT_FOUND);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// try (var fis = new FileInputStream(file)) {
		// 	int n;
		// 	var chunk = new byte[CHUNK_SIZE];
		// 	while ((n = fis.read(chunk)) > 0)
		// 		sink.accept(Arrays.copyOf(chunk, n));

		// 	return ok();
		// } catch (IOException x) {
		// 	return error(INTERNAL_ERROR);
		// }
		return ok();
	}

	@Override
	public Result<Void> delete(String blobId, String token) {
		Log.info(() -> format("delete : blobId = %s, token=%s\n", blobId, token));

		if (!Token.matches(token))
			return error(FORBIDDEN);

		var filePath = toFilePath(blobId);
		IODropbox dropbox = new IODropbox();

		if (filePath == null)
			return error(BAD_REQUEST);

		try {
			if (dropbox.listDirectory(blobFolder(filePath)).contains(filePath))
				return error(NOT_FOUND);
		} catch (Exception e) {
			e.printStackTrace();
		}

		dropbox.delete(filePath);

		return ok();
	}

	@Override
	public Result<Void> deleteAllBlobs(String userId, String token) {
		Log.info(() -> format("deleteAllBlobs : userId = %s, token=%s\n", userId, token));

		if (!Token.matches(token))
			return error(FORBIDDEN);

		try {
			var path = new File(BLOBS_ROOT_DIR + userId);
			Files.walk(path.toPath()).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
			return ok();
		} catch (IOException e) {
			e.printStackTrace();
			return error(INTERNAL_ERROR);
		}
	}

	private boolean validBlobId(String blobId) {
		return Clients.ShortsClients.get().getShort(blobId).isOK();
	}

	private String toFilePath(String blobId) {
		var parts = blobId.split("-");
		if (parts.length != 2)
			return null;

		var res = BLOBS_ROOT_DIR + parts[0] + "/" + parts[1];

		return res;
	}

	private String blobFolder(String url) {
		int lastSlashIndex = url.lastIndexOf('/');
		if (lastSlashIndex == -1) {
			return url;
		}

		return url.substring(0, lastSlashIndex);
	}

	private boolean hasBlob(String filePath, IODropbox dropbox) throws Exception {
		Log.info(blobFolder(filePath));
		Log.info(filePath);
		List<String> files = dropbox.listDirectory(blobFolder(filePath));
		for (String file : files) {
			String path = blobFolder(filePath) + "/" + file;
			if (path.equals(filePath))
				return true;
		}
		return false;
	}
}
