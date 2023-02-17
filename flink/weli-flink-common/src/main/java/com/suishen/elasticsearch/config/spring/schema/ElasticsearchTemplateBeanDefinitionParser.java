package com.suishen.elasticsearch.config.spring.schema;

import com.suishen.elasticsearch.config.spring.ElasticsearchTemplateFactoryBean;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * Author: Bryant Hang
 * Date: 16/6/13
 * Time: 17:31
 */
public class ElasticsearchTemplateBeanDefinitionParser extends AbstractBeanDefinitionParser {

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(ElasticsearchTemplateFactoryBean.class);
        setConfigurations(element, builder);
        return getSourcedBeanDefinition(builder, element, parserContext);
    }

    private void setConfigurations(Element element, BeanDefinitionBuilder builder) {
        builder.addPropertyValue("clusterNodes", element.getAttribute("cluster-nodes"));
        builder.addPropertyValue("clusterName", element.getAttribute("cluster-name"));
    }

    private AbstractBeanDefinition getSourcedBeanDefinition(BeanDefinitionBuilder builder, Element source,
                                                            ParserContext context) {
        AbstractBeanDefinition definition = builder.getBeanDefinition();
        definition.setSource(context.extractSource(source));
        return definition;
    }
}
