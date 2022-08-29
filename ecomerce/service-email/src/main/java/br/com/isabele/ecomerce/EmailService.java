package br.com.isabele.ecomerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class EmailService {
    public static void main(String[] args) {
        var emailService = new EmailService();

        try (var kafkaService = new KafkaService("ECOMMERCE_SEND_EMAIL", emailService::parse,
                EmailService.class.getSimpleName(), String.class,
                new HashMap<>())) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record){
        System.out.println("----------------------------------------");
        System.out.println("Sending email, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email sent");
    }


}
