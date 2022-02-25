package me.zhengjie.config;

import com.cdos.web.htppclient.CdosHttpRestBuilder;
import com.cdos.web.htppclient.CdosHttpRestTemplate;
import com.cdos.web.htppclient.HttpClientPoolProperties;
import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author liuyi
 * @date 2020/8/31
 */
@Configuration
public class HttpClientConfiguration {

    @Bean(name = "awemeHttpRestTemplate")
    public CdosHttpRestTemplate awemeHttpRestTemplate() {

        final HttpClientPoolProperties poolProperties = HttpClientPoolProperties.builder()
            .maxTotal(5)
            .connectionRequestTimeout(1000)
            .connectTimeout(1000)
            .defaultMaxPerRoute(100)
            .socketTimeout(30000)
            .build();

        final CloseableHttpClient httpClient = new CdosHttpRestBuilder().poolProperties(poolProperties)
            .trustAllSsl(true)
            // 开启重试默认3次，注意使用这个方法是天际友盟定制的，默认开启UnknownHostException异常重试
            .retry(true)
            .create()
            .build();

        return new CdosHttpRestTemplate(httpClient);
    }

    @Bean(name = "userHttpRestTemplate")
    public CdosHttpRestTemplate userHttpRestTemplate() {

        final HttpClientPoolProperties poolProperties = HttpClientPoolProperties.builder()
            .maxTotal(5)
            .connectionRequestTimeout(1000)
            .connectTimeout(1000)
            .defaultMaxPerRoute(100)
            .socketTimeout(30000)
            .build();

        final CloseableHttpClient httpClient = new CdosHttpRestBuilder().poolProperties(poolProperties)
            .trustAllSsl(true)
            // 开启重试默认3次，注意使用这个方法是天际友盟定制的，默认开启UnknownHostException异常重试
            .retry(true)
            .create()
            .build();

        return new CdosHttpRestTemplate(httpClient);
    }
}
