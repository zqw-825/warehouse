package com.atguigu.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import javax.naming.InitialContext;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zqw
 * @create 2020-07-27 18:38
 */
public class USTFTest extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //3步
        //校验参数个数
        if (argOIs.getAllStructFieldRefs().size() != 1){
            throw new UDFArgumentException("error");
        }
        //校验参数类型
        if(!"string".equals( argOIs.getAllStructFieldRefs().get(1).getFieldObjectInspector().getTypeName())){
            throw new UDFArgumentException("error2");
        }
        //创建返回值类型检查器 (需要俩ArrayList集合)
        ArrayList<String> fieldNames = new ArrayList<>();//封装字段名字 可多列
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();//封装内容
        fieldNames.add("item");//字段名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);//检查器 直接查
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);//直接查
    }

    @Override
    public void process(Object[] args) throws HiveException {

        //看参数类型 获取想要的
        String string = args[0].toString();
        //json
        JSONArray jsonArray = new JSONArray(string);

        for (int i = 0; i < jsonArray.length(); i++) {

            //输出用数组封装
           String[] arrays = new String[1];

            arrays[0] = jsonArray.getString(i);

            forward (arrays);

        }

    }

    @Override
    public void close() throws HiveException {

    }
}
