package suishen.elasticsearch.config.spring;

import suishen.elasticsearch.core.SuishenElasticsearchTemplate;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;


/**
 * Author: Bryant Hang
 * Date: 16/6/13
 * Time: 17:35
 */
public class ElasticsearchTemplateFactoryBean implements FactoryBean<SuishenElasticsearchTemplate>, InitializingBean, DisposableBean {
    private String clusterNodes = "127.0.0.1:9300";
    private String clusterName = "elasticsearch";

    private SuishenElasticsearchTemplate suishenElasticSearchTemplate;

    @Override
    public void destroy() throws Exception {
        this.suishenElasticSearchTemplate.getClient().close();
        this.suishenElasticSearchTemplate = null;
    }

    @Override
    public SuishenElasticsearchTemplate getObject() throws Exception {
        return this.suishenElasticSearchTemplate;
    }

    @Override
    public Class<?> getObjectType() {
        return SuishenElasticsearchTemplate.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.suishenElasticSearchTemplate = new SuishenElasticsearchTemplate(this.clusterNodes, this.clusterName);
    }

    public String getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(String clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
}
