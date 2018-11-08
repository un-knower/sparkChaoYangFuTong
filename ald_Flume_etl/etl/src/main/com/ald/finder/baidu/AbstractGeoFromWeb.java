package com.ald.finder.baidu;

import com.ald.IpLocation;
import org.apache.http.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.config.*;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.CodingErrorAction;

/**
 * Created by luanma on 2017/5/14.
 */
public abstract class AbstractGeoFromWeb implements GeoFromWeb {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGeoFromWeb.class);
    protected static PoolingHttpClientConnectionManager connManager = null;
    protected CloseableHttpClient httpClient;

    public AbstractGeoFromWeb() {
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.INSTANCE).build();
        connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        SocketConfig socketConfig = SocketConfig.custom().setTcpNoDelay(true).build();
        connManager.setDefaultSocketConfig(socketConfig);
        // Create message constraints
        MessageConstraints messageConstraints = MessageConstraints.custom()
                .setMaxHeaderCount(200)
                .setMaxLineLength(2000)
                .build();
        // Create connection configuration
        ConnectionConfig connectionConfig = ConnectionConfig.custom()
                .setMalformedInputAction(CodingErrorAction.IGNORE)
                .setUnmappableInputAction(CodingErrorAction.IGNORE)
                .setCharset(Consts.UTF_8)
                .setMessageConstraints(messageConstraints)
                .build();
        connManager.setDefaultConnectionConfig(connectionConfig);
        connManager.setMaxTotal(MAX_TOTAL_CONNECTIONS);
        connManager.setDefaultMaxPerRoute(MAX_ROUTE_CONNECTIONS);

        ConnectionKeepAliveStrategy myStrategy = new ConnectionKeepAliveStrategy() {
            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                HeaderElementIterator it = new BasicHeaderElementIterator
                        (response.headerIterator(HTTP.CONN_KEEP_ALIVE));
                while (it.hasNext()) {
                    HeaderElement he = it.nextElement();
                    String param = he.getName();
                    String value = he.getValue();
                    if (value != null && param.equalsIgnoreCase
                            ("timeout")) {
                        return Long.parseLong(value) * 1000;
                    }
                }
                return 5 * 1000;
            }
        };

        httpClient = HttpClients.custom()
                .setConnectionManager(connManager)
                .setKeepAliveStrategy(myStrategy)
                .build();
    }

    /**
     * 根据不同的实现来拼取url，含指定参数
     *
     * @param ip
     * @return
     */
    protected abstract String getSearchUrl(String ip);

    /**
     * 根据不同的实现解析不同的返回结果，并返回IP所需的IPLocation，如果没有查询到，则返回null
     *
     * @param content
     * @return
     */
    protected abstract IpLocation parseContent(String content);

    public IpLocation find(String ip) {
        try {
            HttpRequestBase login = (HttpRequestBase) RequestBuilder.get(getSearchUrl(ip))
                    .build();
            CloseableHttpResponse response = httpClient.execute(login);
            try {
                HttpEntity entity = response.getEntity();
                int status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    IpLocation ipLocation = parseContent(EntityUtils.toString(entity));
                    return ipLocation;
                }
                EntityUtils.consume(entity);
            } finally {
                try {
                    response.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                login.releaseConnection();
            }
        } catch (Exception e) {
            logger.error("LSF was error", e);
        }
        return null;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        HttpClientUtils.closeQuietly(httpClient);
        connManager.close();
        connManager.shutdown();
    }
}
