package suishen.elasticsearch.meta.meta.query;



import suishen.elasticsearch.meta.meta.req.BoolType;

import java.util.Arrays;
import java.util.List;

/**
 * Author: Alvin Li
 * Date: 7/12/16
 * Time: 11:15
 */
public class StringTermMeta {
    /**
     * 对应ES中的field
     */
    private String field;

    /**
     * field对应的查询值
     */
    private List<String> values;

    private BoolType boolType;


    public StringTermMeta(String field,BoolType boolType, String... values) {
        this.field = field;
        this.values = Arrays.asList(values);
        this.boolType = boolType;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public BoolType getBoolType() {
        return boolType;
    }

    public void setBoolType(BoolType boolType) {
        this.boolType = boolType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StringTermMeta that = (StringTermMeta) o;

        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        if (values != null ? !values.equals(that.values) : that.values != null) return false;
        return boolType == that.boolType;
    }

    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (values != null ? values.hashCode() : 0);
        result = 31 * result + (boolType != null ? boolType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "StringTermMeta{" +
                "field='" + field + '\'' +
                ", values=" + values +
                ", boolType=" + boolType +
                '}';
    }
}
