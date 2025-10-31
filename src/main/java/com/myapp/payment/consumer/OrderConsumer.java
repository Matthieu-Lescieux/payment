package com.myapp.payment.consumer;

import com.myapp.orders.dto.Order;
import com.myapp.payment.dto.Payment;
import com.myapp.payment.producer.PaymentProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class OrderConsumer {

    private final PaymentProducer paymentProducer;

    @KafkaListener(topics = "order-created", groupId = "payment")
    public void listenOrderCreated(Order order) {
        System.out.println("Received Message in group payment: " + order.getOrderId());
        final Payment payment = new Payment(order.getOrderId(), order.getOrderId(), "SUCCESS");
        paymentProducer.sendPayment(payment);
    }
}
