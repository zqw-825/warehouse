package com.atguigu.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;

/**
 * @author zqw
 * @create 2020-07-27 9:32
 */
public class ExplodeJsonArray extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //校验参数个数
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentException("参数个数错误");
        }
        //校验参数类型 有几个校验几个
        if(!"string".equals( argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName())){
            throw new UDFArgumentException("参数类型不是String");
        }
        //返回函数的输出类型对应的检查器
        //创建2个ArrayList集合
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("item");//字段名字
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);//校验 javaString检查器
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);

    }

    @Override
    public void process(Object[] args) throws HiveException {
        //获取要用的
        String jsonArrayStr = args[0].toString();
        //转换为json对象
        JSONArray jsonArray = new JSONArray(jsonArrayStr);
        for (int i = 0; i < jsonArray.length(); i++) {

            String[] result = new String[1];//封装每行的数组  这里长度为1

            result[0] = jsonArray.getString(i);//给数组赋值

            forward(result);//输出数组 forward

        }

    }

    @Override
    public void close() throws HiveException {

    }
}
