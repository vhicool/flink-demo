package flink.demo.sql.mysql2kafka;

import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * User Schema ，支持序列化和反序列化
 * <p>
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class StudentSchema implements DeserializationSchema<Student>, SerializationSchema<Student> {

    private static final Gson gson = new Gson();

    @Override
    public Student deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), Student.class);
    }

    @Override
    public boolean isEndOfStream(Student student) {
        return false;
    }

    @Override
    public byte[] serialize(Student student) {
        return gson.toJson(student).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<Student> getProducedType() {
        return TypeInformation.of(Student.class);
    }
}
