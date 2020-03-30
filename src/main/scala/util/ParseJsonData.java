package util;

import com.alibaba.fastjson.JSONObject;

public class ParseJsonData {
    //数据是json格式的就返回，非json格式就报错
    public static JSONObject getJsonData(String data){
        try {
            return JSONObject.parseObject(data);
        }catch (Exception e){
            return null;
        }
    }
}
