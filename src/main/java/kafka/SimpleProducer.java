package kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import utils.DateConvert;

import java.util.Properties;
/**
 * Created by shaosong on 2017/4/20.
 */
public class SimpleProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("metadata.broker.list","localhost:9092");
        props.setProperty("serializer.class","kafka.serializer.StringEncoder");
        props.put("request.required.acks","1");
        ProducerConfig config = new ProducerConfig(props);
        //创建生产这对象
        Producer<String, String> producer = new Producer<String, String>(config);
        try {
            int i =1;
            while(true){
                //发送消息
                //生成消息
                KeyedMessage<String, String> data = new KeyedMessage<String, String>("sstest","test kafka " + DateConvert.getCurrentDate());
                producer.send(data);
                i++;
                Thread.sleep(1000);
                System.out.println("send data: " + data);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
