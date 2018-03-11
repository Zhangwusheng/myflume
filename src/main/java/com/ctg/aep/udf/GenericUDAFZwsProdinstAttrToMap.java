package com.ctg.aep.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/***
 * zws 2018-03-11
 * 功能：竖表转横表
 * 限制：所有的输入列都需要转换为string
 * 输入数据：
 * prod_inst_id1,功能产品1，产品特性ID1，产品特性值1
 * prod_inst_id1,功能产品2，产品特性ID2，产品特性值2
 * prod_inst_id2,功能产品3，产品特性ID2，产品特性值2
 * prod_inst_id2,功能产品4，产品特性ID3，产品特性值3
 * 输出数据：
 * prod_inst_id1,{"功能产品1":{"产品特性ID1":"产品特性值1"},"功能产品2":{"产品特性ID2":"产品特性值2"}}
 * prod_inst_id2,{"功能产品3":{"产品特性ID2":"产品特性值2"},"功能产品4":{"产品特性ID3":"产品特性值3"}}
 *
 * 使用方法：
     hdfs dfs -rm /tmp/udf-1.0.jar
     hdfs dfs -put /home/odp/udf-1.0.jar /tmp
     CREATE temporary FUNCTION zws AS 'com.ctg.dw.udf.GenericUDAFZwsProdinstAttrToMap' USING JAR 'hdfs://odptest/tmp/udf-1.0.jar';
     create table zws_prod_inst_attr_map_test
     as
     select prod_inst_id,zws( cast(attr_id as string), cast(attr_value_id as string),attr_value)  as attr_map
     from merge.prod_inst_attr  where yyyymmdd=20180210 group by prod_inst_id;
 */
@Description(name = "Vertical2Horizontal ", value = "_FUNC_(x) -Transform Vertical table to horizontal table using pap<string,map<string,string> structure")
public class GenericUDAFZwsProdinstAttrToMap extends AbstractGenericUDAFResolver {
    static final Logger LOG = LoggerFactory.getLogger(GenericUDAFZwsProdinstAttrToMap.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 3) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly four argument is expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " is passed.");
        }

        if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1,
                    "Only primitive type arguments are accepted but "
                            + parameters[1].getTypeName() + " is passed.");
        }

        if (parameters[2].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(2,
                    "Only primitive type arguments are accepted but "
                            + parameters[2].getTypeName() + " is passed.");
        }

        return new ProdInstAttrMapEvaluator();

//        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
//            case LONG:
//                return new GenericUDAFSum.GenericUDAFSumLong();
//            case TIMESTAMP:
//            case FLOAT:
//            case DOUBLE:
//            case STRING:
//            case VARCHAR:
//            case CHAR:
//            case DECIMAL:
//            case BOOLEAN:
//            case DATE:
//            default:
//                throw new UDFArgumentTypeException(0,
//                        "Only numeric type for argument 0 are accepted but "
//                                + parameters[0].getTypeName() + " is passed.");
//        }
    }


    public static class ProdInstAttrMapEvaluator extends GenericUDAFEvaluator {

        protected PrimitiveObjectInspector inputObjectInspectorOfOuterKey;
        protected PrimitiveObjectInspector inputObjectInspectorOfInnerKey;
        protected PrimitiveObjectInspector inputObjectInspectorOfInnerValue;
        protected StandardMapObjectInspector mapObjectInspector;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1) {
                inputObjectInspectorOfOuterKey = (PrimitiveObjectInspector) parameters[0];
                inputObjectInspectorOfInnerKey = (PrimitiveObjectInspector) parameters[1];
                inputObjectInspectorOfInnerValue = (PrimitiveObjectInspector) parameters[2];

                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                        , ObjectInspectorFactory.getStandardMapObjectInspector(
                                ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
                                ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                        ));
            } else if (m == Mode.PARTIAL2) {
                mapObjectInspector = (StandardMapObjectInspector) parameters[0];

                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                        , ObjectInspectorFactory.getStandardMapObjectInspector(
                                ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
                                ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                        ));
            } else if (m == Mode.FINAL) {
                mapObjectInspector = (StandardMapObjectInspector) parameters[0];

                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                        , ObjectInspectorFactory.getStandardMapObjectInspector(
                                ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
                                ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                        ));
            } else if (m == Mode.COMPLETE) {
                mapObjectInspector = (StandardMapObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                        , ObjectInspectorFactory.getStandardMapObjectInspector(
                                ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
                                ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                        ));
            } else {
                throw new RuntimeException("Ctg:no such mode Exception");
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new ProdInstAttrMapBuffer();
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ProdInstAttrMapBuffer prodInstAttrMapBuffer = (ProdInstAttrMapBuffer) agg;
            prodInstAttrMapBuffer.reset();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            ProdInstAttrMapBuffer prodInstAttrMapBuffer = (ProdInstAttrMapBuffer) agg;

            Object outKeyObject = parameters[0];
            Object innerKeyObject = parameters[1];
            Object innerValueObject = parameters[2];

            String outerKey = PrimitiveObjectInspectorUtils.getString(outKeyObject, inputObjectInspectorOfOuterKey);
            String innerKey = PrimitiveObjectInspectorUtils.getString(innerKeyObject, inputObjectInspectorOfInnerKey);
            String value = PrimitiveObjectInspectorUtils.getString(innerValueObject, inputObjectInspectorOfInnerValue);

            prodInstAttrMapBuffer.add(outerKey, innerKey, value);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            ProdInstAttrMapBuffer prodInstAttrMapBuffer = (ProdInstAttrMapBuffer) agg;

            Map<String, Map<String, String>> partialResult = new HashMap<>(prodInstAttrMapBuffer.results);
            return partialResult;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            ProdInstAttrMapBuffer prodInstAttrMapBuffer = (ProdInstAttrMapBuffer) agg;

            Map<Object, Object> partialResult = (Map<Object, Object>) mapObjectInspector.getMap(partial);
            for (Map.Entry<Object, Object> objectObjectEntry : partialResult.entrySet()) {
                Object inner = mapObjectInspector.getMapValueElement(partial, objectObjectEntry.getKey());
                LazyBinaryMap lazyBinaryMap = (LazyBinaryMap) (inner);

                Map<Object, Object> mmm = lazyBinaryMap.getMap();
                Object outKeyObject = objectObjectEntry.getKey();
                if (outKeyObject == null)
                    continue;
                String outKey = outKeyObject.toString();

                if (prodInstAttrMapBuffer.results.containsKey(outKey)) {
                    for (Map.Entry<Object, Object> innerEntry : mmm.entrySet()) {
                        Object o1 = innerEntry.getKey();
                        Object o2 = innerEntry.getValue();
                        String str1 = "";
                        String str2 = "";
                        if (o1 == null)
                            continue;
                        else
                            str1 = o1.toString();
                        if (o2 != null)
                            str2 = o2.toString();

                        prodInstAttrMapBuffer.results.get(outKey).put(str1, str2);
                    }
                } else {
                    Map<String, String> innerMap = new HashMap<>();
                    prodInstAttrMapBuffer.results.put(outKey, innerMap);
                    for (Map.Entry<Object, Object> innerEntry : mmm.entrySet()) {
                        Object o1 = innerEntry.getKey();
                        Object o2 = innerEntry.getValue();
                        String str1 = "";
                        String str2 = "";
                        if (o1 == null)
                            continue;
                        else
                            str1 = o1.toString();
                        if (o2 != null)
                            str2 = o2.toString();

                        innerMap.put(str1, str2);
                    }
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ProdInstAttrMapBuffer prodInstAttrMapBuffer = (ProdInstAttrMapBuffer) agg;
            Map<String, Map<String, String>> partialResult = new HashMap<>(prodInstAttrMapBuffer.results);

            return partialResult;
        }

        public static class ProdInstAttrMapBuffer extends AbstractAggregationBuffer {

            protected Map<String, Map<String, String>> results;

            public ProdInstAttrMapBuffer() {
                results = new HashMap<>(10);
            }

            public void add(String oKey, String iKey, String value) {
                if (results.containsKey(oKey)) {
                    results.get(oKey).put(iKey, value);
                } else {
                    Map<String, String> innerMap = new HashMap<>();
                    innerMap.put(iKey, value);
                    results.put(oKey, innerMap);
                }
            }

            public void reset() {
                results.clear();
                results = null;
                results = new HashMap<>(10);
            }
        }
    }
}
