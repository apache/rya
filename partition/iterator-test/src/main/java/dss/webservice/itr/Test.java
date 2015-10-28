package dss.webservice.itr;

import java.util.Map;

import cloudbase.core.client.Connector;

public interface Test {
	void runTest(Map<String, String> request, Connector connector, String table, String auths);
}
