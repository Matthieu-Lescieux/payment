package com.myapp.payment.producer;

import com.myapp.payment.dto.Payment;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
public class PaymentProducer {

    private static final String TOPIC = "payments";
    private final KafkaTemplate<String, Payment> kafkaTemplate;

    public PaymentProducer(KafkaTemplate<String, Payment> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPayment(Payment payment) {
        CompletableFuture<SendResult<String, Payment>> pendingResult = kafkaTemplate.send(TOPIC, payment.getPaymentId(), payment);
        pendingResult.whenComplete((r, e) -> {
            RecordMetadata metadata = r.getRecordMetadata();
            log.info("Sent payment {} to partition {} offset {}", payment.getPaymentId(), metadata.partition(), metadata.offset());
        });
    }
}
