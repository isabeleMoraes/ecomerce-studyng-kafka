package br.com.isabele.ecomerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try(var orderDispacher = new KafkaDispatcher<Order>()){
            try(var emailDispacher = new KafkaDispatcher<String>()){
                for (var i = 0; i<10; i++){
                    //Colocando uma chave diferente pois o kafka utiliza um algoritimo hash para balancear as partições e as instancias
                    String userId = UUID.randomUUID().toString();
                    String orderId = UUID.randomUUID().toString();
                    BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);

                    var order = new Order(userId,orderId,amount);
                    orderDispacher.send("ECOMMERCE_NEW_ORDER", userId, order);

                    var email = new Email("subject", "We are processing your order!!");
                    emailDispacher.send("ECOMMERCE_SEND_EMAIL", userId, "Mensagem do email");
                }
            }
        }
    }
}
