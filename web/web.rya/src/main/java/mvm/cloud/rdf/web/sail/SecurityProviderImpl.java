package mvm.cloud.rdf.web.sail;

import javax.servlet.http.HttpServletRequest;

import mvm.rya.api.security.SecurityProvider;

public class SecurityProviderImpl implements SecurityProvider{
	
	public String[] getUserAuths(HttpServletRequest incRequest) {
		String[] auths = incRequest.getParameterValues("query.auth");
		return auths;
	}

}
