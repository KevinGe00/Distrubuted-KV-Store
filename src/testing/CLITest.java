package testing;

import app_kvClient.KVClient;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class CLITest extends TestCase {

	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	private final PrintStream originalOut = System.out;

	@Before
	public void setUpStreams() {
		System.setOut(new PrintStream(outContent));
	}

	@After
	public void restoreStreams() {
		System.setOut(originalOut);
	}
	@Test
	public void testInvalidPort() {
		ByteArrayOutputStream outContent = new ByteArrayOutputStream();
		System.setOut(new PrintStream(outContent));

		Exception ex = null;

		try {
			KVClient kvClient = new KVClient();
			kvClient.handleCommand("connect localhost portshouldbenum");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(outContent.toString().contains("No valid port. Port must be a number"));

		System.setOut(System.out);
	}

	@Test
	public void testInvalidHost() {
		ByteArrayOutputStream outContent = new ByteArrayOutputStream();
		System.setOut(new PrintStream(outContent));

		Exception ex = null;

		try {
			KVClient kvClient = new KVClient();
			kvClient.handleCommand("connect notarealhost 8000");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(outContent.toString().contains("Unknown Host"));

		System.setOut(System.out);
	}

	@Test
	public void testInvalidNumParams() {
		ByteArrayOutputStream outContent = new ByteArrayOutputStream();
		System.setOut(new PrintStream(outContent));

		Exception ex = null;

		try {
			KVClient kvClient = new KVClient();
			kvClient.handleCommand("connect one two three");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(outContent.toString().contains("Invalid number of parameters!"));

		System.setOut(System.out);
	}

	public void testChangeLogLevel() {
		ByteArrayOutputStream outContent = new ByteArrayOutputStream();
		System.setOut(new PrintStream(outContent));

		Exception ex = null;

		try {
			KVClient kvClient = new KVClient();
			kvClient.handleCommand("logLevel DEBUG");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(outContent.toString().contains("Log level changed to level"));

		System.setOut(System.out);
	}
	public void testInvalidLogLevel() {
		ByteArrayOutputStream outContent = new ByteArrayOutputStream();
		System.setOut(new PrintStream(outContent));

		Exception ex = null;

		try {
			KVClient kvClient = new KVClient();
			kvClient.handleCommand("logLevel asdfsad");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(outContent.toString().contains("No valid log level!"));

		System.setOut(System.out);
	}
}
