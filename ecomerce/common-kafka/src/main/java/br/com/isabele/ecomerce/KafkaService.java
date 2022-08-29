package br.com.isabele.ecomerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String topic, ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> properties) {
        this(parse,groupId, type, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(Pattern topic, ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> properties) {
        this(parse,groupId, type, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer(getProperties(groupId, type, properties));
    }

    public void run() {
        while (true){
            ConsumerRecords<String, T> consumerRecords = consumer.poll(Duration.ofMillis(100));
            if(!consumerRecords.isEmpty()){
                System.out.println("Encontrei " + consumerRecords.count()+ " registros");
                for(var record : consumerRecords){
                    parse.consume(record);
                }
            }
        }
    }

    private Properties getProperties(String groupId, Class<T> type, Map<String, String> overrideProperties) {
        var properties = new Properties();

        //Estou setando uma propriedade que fala para o consumidor qual o endereço do servidor.
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //Estou dizendo como deve ser deserializada a chave
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //Estou dizendo como deve ser deserializada a mensagem (O valor consumido)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        // Precisamos definir um grupo para o consumidor para que ele possa receber todas as mensagens
        // Grupos com mais de 1 consumidor, dividem as mensagens e não conseguimos garantir que foram processadas por esse consumidor ou pelo outro.
        // Por isso cada consumidor tem que ter seu grupo
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //Podemos colocar o nome do client id que nada mais é que a instancia do cliente que está consumindo a  mensagem.
        // Esse id precisa ser unico.
        // No caso de ter mais de uma instancia desse consumer rodando, terei como identificar cada uma.
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

        // Falando o maximo de mensagem que o consumidor irá consumir antes de realizar o commit.
        // Sem essa configuração, o consumidor só vai dar o commit (Ou seja, atualizar o nomedo de mensagens consumidas até o momento nas partições)
        // quando tiver terminado de consumir todas as mensagens. Isso é ruim pq o Kafka pode rebalancear as partições de repente e bagunçar tudo.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        //Para conseguir falar para o deserializador qual o tipo da classe que vamos deserializar
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());

        //Dessa forma, vamos sobrescrever tudo que estiver com o mesmo nome de propriedades e contiver no map passado
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close(){
        consumer.close();
    }
}
