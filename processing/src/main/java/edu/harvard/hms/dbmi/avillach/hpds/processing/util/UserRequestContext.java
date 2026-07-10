package edu.harvard.hms.dbmi.avillach.hpds.processing.util;

import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;

import java.util.List;

@Component
@Scope(value = WebApplicationContext.SCOPE_REQUEST, proxyMode = ScopedProxyMode.TARGET_CLASS)
// Or simply use the shortcut: @RequestScope
public class UserRequestContext {
    private List<String> userConsents = List.of();

    public List<String> getUserConsents() {
        return userConsents;
    }

    public UserRequestContext setUserConsents(List<String> userConsents) {
        this.userConsents = userConsents;
        return this;
    }
}
