package utils.DropBoxMsgs;

public record ListFolderArgs( String path, boolean recursive, boolean include_media_info, boolean include_deleted, boolean include_has_explicit_shared_members, boolean include_mounted_folders) {
	public ListFolderArgs( String path) {
		this( path, false, false, false, false, false);
	}
}
