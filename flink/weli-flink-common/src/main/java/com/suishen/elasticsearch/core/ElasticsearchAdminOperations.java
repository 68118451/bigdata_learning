package com.suishen.elasticsearch.core;

import java.util.List;

/**
 * Author: Alvin Li
 * Date: 7/6/16
 * Time: 19:06
 */
public interface ElasticsearchAdminOperations {



    /**
     * modify alias (add/delete)
     * @param add
     * @param remove
     */
    boolean modifyAlias(AliasIndexHolder[] add, AliasIndexHolder[] remove);

    /**
     * Get indices by alias
     * @param  alias alias name
     */
    List<String> getIndicesByAliasName(String alias);


    /**
     * Get indices name by pattern
     * @param pattern regrex string. null return all
     * @return
     */
    List<String> getIndicesByPattern(String pattern);
    /**
     * check index's existence
     * @param index index name
     */
    boolean isIndexExist(String index);

    /**
     * check alias's existence
     * @param alias alias name
     */
    boolean isAliasExist(String alias);

    /**
     * get all alias name
     *
     */
    List<String> getAllAlias();

    /**
     * create index
     * @param index index name
     * @param alias alias name
     */
    boolean createIndex(String index,String alias);

    /**
     * delete index
     *
     */
    boolean deleteIndex(String... index);

    /**
     * Optimize index
     * @param name indices name
     */
    void optimizeIndex(String... name);

    /**
     * 创建template
     * @param templateName
     * @param template
     * @return
     */
    boolean createTemplate(String templateName,String template);

    /**
     * 删除template
     * @param templateName
     * @return
     */
    boolean deleteTemplate(String templateName);
}
