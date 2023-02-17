package com.suishen.elasticsearch.config.spring.schema;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * Author: Bryant Hang
 * Date: 16/6/13
 * Time: 17:29
 */
public class ElasticsearchNamespaceHandler extends NamespaceHandlerSupport {

    public void init() {
        registerBeanDefinitionParser("template", new ElasticsearchTemplateBeanDefinitionParser());
    }

}
