package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;

import java.net.UnknownHostException;


public class ConnectionTest extends TestCase {
	@Test
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
		KVStore kvClient = new KVStore("localhost", 7000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	@Test
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}
	
	@Test
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}
	
	

	
}

