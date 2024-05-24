package utils.DropBoxMsgs;

import java.util.HashMap;
import java.util.List;

public class ListFolderReturn {

	private String cursor;
	private boolean has_more;
	private List<FolderEntry> entries;
		
	public ListFolderReturn() {	
	}
	
	public String getCursor() {
		return cursor;
	}

	public void setCursor(String cursor) {
		this.cursor = cursor;
	}

	public boolean has_more() {
		return has_more;
	}

	public void setHas_more(boolean has_more) {
		this.has_more = has_more;
	}

	public List<FolderEntry> getEntries() {
		return entries;
	}

	public void setEntries(List<FolderEntry> entries) {
		this.entries = entries;
	}
	
	public static class FolderEntry extends HashMap<String, Object>{
		private static final long serialVersionUID = 1L;
		private static final String NAME = "name";

		public String toString() {
			return get(NAME).toString();
		}
	}
}
