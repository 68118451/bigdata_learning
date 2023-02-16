package suishen.elasticsearch.rest.client.config;

import java.io.Serializable;

/**
 * Author: Alvin Li
 * Date: 09/08/2017
 * Time: 18:36
 */
public class HttpHostConfig implements Serializable {

    private static final long serialVersionUID = -8893211191105187987L;
    private String host;

    private int port;

    private String schema;

    public HttpHostConfig() {
    }

    public HttpHostConfig(String host, int port, String schema) {
        this.host = host;
        this.port = port;
        this.schema = schema;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static final class Builder {
        private String host;
        private int port;
        private String schema;

        private Builder() {
        }


        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setSchema(String schema) {
            this.schema = schema;
            return this;
        }

        public HttpHostConfig build() {
            HttpHostConfig httpHostConfig = new HttpHostConfig();
            httpHostConfig.setHost(host);
            httpHostConfig.setPort(port);
            httpHostConfig.setSchema(schema);
            return httpHostConfig;
        }
    }
}
