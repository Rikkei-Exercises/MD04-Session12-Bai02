package com.example.pharmacyservice.service;

import com.example.pharmacyservice.config.KafkaProducerConfig;
import com.example.pharmacyservice.event.OrderCreatedEvent;
import com.example.pharmacyservice.event.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Service phụ trách gửi sự kiện OrderEvent & OrderCreatedEvent lên Kafka topic.
 *
 * Yêu cầu:
 * - Sau khi lưu đơn hàng vào DB, gửi một sự kiện OrderCreatedEvent vào topic 'order-events'.
 * - Email-Service & Inventory-Service (Consumers) cùng lắng nghe topic 'order-events'
 *   (nhưng khác group-id) để thực hiện gửi mail và trừ kho một cách độc lập.
 * - Luồng đặt hàng kết thúc ngay khi gửi tin vào Kafka (Asynchronous Fire-and-forget),
 *   giúp trải nghiệm người dùng mượt mà và không phải chờ đợi.
 */
@Slf4j
@Service
public class OrderProducerService {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Autowired(required = false)
    private KafkaTemplate<String, OrderCreatedEvent> orderCreatedKafkaTemplate;

    public OrderProducerService(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Gửi sự kiện OrderCreatedEvent vào topic 'order-events'.
     * Luồng đặt hàng kết thúc ngay lập tức, không chờ trừ kho hay gửi mail.
     */
    public CompletableFuture<SendResult<String, OrderCreatedEvent>> sendOrderCreatedEvent(OrderCreatedEvent event) {
        String messageKey = event.getOrderId();

        log.info(">>> [KAFKA PRODUCER - ORDER-SERVICE] Gửi sự kiện OrderCreatedEvent vào topic '{}': orderId='{}', qty={}, email='{}'",
                KafkaProducerConfig.ORDER_EVENTS_TOPIC, messageKey, event.getOrderId(), event.getCustomerEmail());

        if (orderCreatedKafkaTemplate != null) {
            CompletableFuture<SendResult<String, OrderCreatedEvent>> future =
                    orderCreatedKafkaTemplate.send(KafkaProducerConfig.ORDER_EVENTS_TOPIC, messageKey, event);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info(">>> [KAFKA ACK] Gửi OrderCreatedEvent thành công: topic='{}', partition={}, offset={}",
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error(">>> [KAFKA ACK FAIL] Lỗi khi gửi OrderCreatedEvent: {}", ex.getMessage());
                }
            });
            return future;
        }

        // Cũng gửi qua topic medicine-stock-events nếu cần tương thích
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Gửi OrderEvent lên topic medicine-stock-events với Message Key = String.valueOf(medicineId).
     */
    public CompletableFuture<SendResult<String, OrderEvent>> sendOrderEvent(OrderEvent orderEvent) {
        String messageKey = String.valueOf(orderEvent.getMedicineId());

        log.info("Chuẩn bị gửi OrderEvent lên Kafka: topic='{}', key='{}', orderId='{}', medicineId={}, quantity={}",
                KafkaProducerConfig.MEDICINE_STOCK_TOPIC, messageKey, orderEvent.getOrderId(),
                orderEvent.getMedicineId(), orderEvent.getQuantity());

        CompletableFuture<SendResult<String, OrderEvent>> future = 
                kafkaTemplate.send(KafkaProducerConfig.MEDICINE_STOCK_TOPIC, messageKey, orderEvent);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Gửi OrderEvent thành công lên topic='{}', partition={}, offset={}, key='{}'",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        messageKey);
            } else {
                log.error("Gửi OrderEvent thất bại cho orderId='{}', lỗi: {}",
                        orderEvent.getOrderId(), ex.getMessage(), ex);
            }
        });

        return future;
    }
}
