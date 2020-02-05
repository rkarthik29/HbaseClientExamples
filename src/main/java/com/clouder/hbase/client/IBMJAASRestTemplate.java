package com.clouder.hbase.client;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

/**
 * {@code RestTemplate} that is able to make kerberos SPNEGO authenticated REST requests. Under a hood this {@code KerberosRestTemplate} is using {@link HttpClient} to support Kerberos.
 *
 * <p>
 * Generally this template can be configured in few different ways.
 * <ul>
 * <li>Leave keyTabLocation and userPrincipal empty if you want to use cached ticket</li>
 * <li>Use keyTabLocation and userPrincipal if you want to use keytab file</li>
 * <li>Use userPrincipal and password if you want to use user/password</li>
 * <li>Use loginOptions if you want to customise Krb5LoginModule options</li>
 * <li>Use a customised httpClient</li>
 * </ul>
 *
 * @author Janne Valkealahti
 *
 */
public class IBMJAASRestTemplate extends RestTemplate {

    private static final Credentials credentials = new NullCredentials();

    private final String keyTabLocation;
    private final String userPrincipal;
    private final String password;
    private final Map<String, Object> loginOptions;

    /**
     * Instantiates a new kerberos rest template.
     *
     * @param keyTabLocation
     *            the key tab location
     * @param userPrincipal
     *            the user principal
     * @param loginOptions
     *            the login options
     */
    public IBMJAASRestTemplate(String keyTabLocation, String userPrincipal, Map<String, Object> loginOptions) {
        this(keyTabLocation, userPrincipal, loginOptions, buildHttpClient());
    }

    /**
     * Instantiates a new kerberos rest template.
     *
     * @param keyTabLocation
     *            the key tab location
     * @param userPrincipal
     *            the user principal
     * @param password
     *            the password
     * @param loginOptions
     *            the login options
     * @param httpClient
     *            the http client
     */
    private IBMJAASRestTemplate(String keyTabLocation, String userPrincipal, Map<String, Object> loginOptions, HttpClient httpClient) {
        super(new HttpComponentsClientHttpRequestFactory(httpClient));
        this.keyTabLocation = keyTabLocation;
        this.userPrincipal = userPrincipal;
        this.loginOptions = loginOptions;
        this.password = null;
    }

    /**
     * Builds the default instance of {@link HttpClient} having kerberos support.
     *
     * @return the http client with spneno auth scheme
     */
    private static HttpClient buildHttpClient() {
        HttpClientBuilder builder = HttpClientBuilder.create();
        Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider> create().register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build();
        builder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(null, -1, null), credentials);
        builder.setDefaultCredentialsProvider(credentialsProvider);
        CloseableHttpClient httpClient = builder.build();
        return httpClient;
    }

    /**
     * Setup the {@link LoginContext} with credentials and options for authentication against kerberos.
     *
     * @return the login context
     */
    private LoginContext buildLoginContext() throws LoginException {
        ClientLoginConfig loginConfig = new ClientLoginConfig(keyTabLocation, userPrincipal, this.password, loginOptions);
        Set<Principal> princ = new HashSet<Principal>(1);
        princ.add(new KerberosPrincipal(userPrincipal));
        Subject sub = new Subject(false, princ, new HashSet<Object>(), new HashSet<Object>());
        CallbackHandler callbackHandler = new CallbackHandlerImpl(userPrincipal, this.password);
        LoginContext lc = new LoginContext("", sub, callbackHandler, loginConfig);
        System.out.println("logged in" + lc.getSubject());
        return lc;
    }

    @Override
    protected final <T> T doExecute(final URI url, final HttpMethod method, final RequestCallback requestCallback, final ResponseExtractor<T> responseExtractor) throws RestClientException {

        try {
            LoginContext lc = buildLoginContext();
            lc.login();
            Subject serviceSubject = lc.getSubject();
            return Subject.doAs(serviceSubject, new PrivilegedAction<T>() {

                public T run() {
                    // TODO Auto-generated method stub
                    System.out.println(url + "--" + method);
                    return IBMJAASRestTemplate.this.doExecuteSubject(url, method, requestCallback, responseExtractor);
                }
            });

        } catch (Exception e) {
            throw new RestClientException("Error running rest call", e);
        }
    }

    private <T> T doExecuteSubject(URI url, HttpMethod method, RequestCallback requestCallback, ResponseExtractor<T> responseExtractor) throws RestClientException {
        try {
            return super.doExecute(url, method, requestCallback, responseExtractor);
        } catch (RestClientException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            throw e;
        }
    }

    private static class ClientLoginConfig extends Configuration {

        private final String keyTabLocation;
        private final String userPrincipal;
        private final String password;
        private final Map<String, Object> loginOptions;

        private ClientLoginConfig(String keyTabLocation, String userPrincipal, String password, Map<String, Object> loginOptions) {
            super();
            this.keyTabLocation = keyTabLocation;
            this.userPrincipal = userPrincipal;
            this.loginOptions = loginOptions;
            this.password = password;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {

            Map<String, Object> options = new HashMap<String, Object>();

            // if we don't have keytab or principal only option is to rely on
            // credentials cache.
            if (!StringUtils.hasText(keyTabLocation) || !StringUtils.hasText(userPrincipal)) {
                // cache
                options.put("useDefaultCcache", "true");
            } else {
                // keytab
                options.put("credsType", "both");
                options.put("useKeytab", keyTabLocation);
                options.put("principal", this.userPrincipal);
            }

            if (loginOptions != null) {
                options.putAll(loginOptions);
            }

            return new AppConfigurationEntry[] { new AppConfigurationEntry("com.ibm.security.auth.module.Krb5LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
        }

    }

    private static class NullCredentials implements Credentials {

        public Principal getUserPrincipal() {
            // TODO Auto-generated method stub
            return null;
        }

        public String getPassword() {
            // TODO Auto-generated method stub
            return null;
        }

    }

    private static class CallbackHandlerImpl implements CallbackHandler {

        private final String userPrincipal;
        private final String password;

        private CallbackHandlerImpl(String userPrincipal, String password) {
            super();
            this.userPrincipal = userPrincipal;
            this.password = password;
        }

        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            // TODO Auto-generated method stub

            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nc = (NameCallback) callback;
                    nc.setName(userPrincipal);
                } else if (callback instanceof PasswordCallback) {
                    PasswordCallback pc = (PasswordCallback) callback;
                    pc.setPassword(password.toCharArray());
                } else {
                    throw new UnsupportedCallbackException(callback, "Unknown Callback");
                }
            }
        }

    }
}
