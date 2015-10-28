package mvm.rya.api.security;

import javax.servlet.http.HttpServletRequest;

public interface SecurityProvider {

	public String[] getUserAuths(HttpServletRequest incRequest);
}
