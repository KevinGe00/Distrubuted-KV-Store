package testing;

import app_kvServer.Store;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;

public class StoreTest extends TestCase {
	
	// TODO add your test cases, at least 3
	private void deleteTestDir() {
		String dirPath = "test";
		File folder = new File(dirPath);

		File[] files = folder.listFiles();
		if (files != null) {
			for (File file : files) {
				file.delete();
			}
		}

		folder.delete();
	}
	@Test
	public void testNewDirectoryIsCreatedIfNeeded() throws Exception {
		String dirPath = "test";
		File folder = new File(dirPath);
		assertFalse(folder.exists());

		new Store(0, 0, "test");
		assertTrue(folder.exists() && folder.isDirectory());

		deleteTestDir();
	}

	public void testPutAndPersistence() throws Exception {
		String dirPath = "test";
		Store store = new Store(0, 0, "test");
		File folder = new File(dirPath);

		store.put("name", "john");

		String key;
		String value = "null";

		for (File file : folder.listFiles()) {
			key = file.getName();
			if (key == "name") {
				value = store.readContent(file);
			}
		}

		assert value == "john";

		deleteTestDir();
	}

	public void testClearStorage() throws Exception {
		String dirPath = "test";
		Store store = new Store(0, 0, "test");
		File folder = new File(dirPath);

		store.put("name", "john");
		store.put("job", "student");

		assert folder.listFiles().length == 2;

		store.clearStorage();

		assert folder.listFiles().length == 0;

		deleteTestDir();
	}

	public void testGetFromStorage() throws Exception {
		String dirPath = "test";
		Store store = new Store(0, 0, dirPath);

		store.put("name", "john");

		String val = store.get("name");

		assert val == "john";

		deleteTestDir();
	}
}
