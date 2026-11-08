package com.example.pharmacyservice;

import com.example.pharmacyservice.controller.OrderController;
import com.example.pharmacyservice.dto.OrderRequest;
import com.example.pharmacyservice.event.OrderCreatedEvent;
import com.example.pharmacyservice.event.OrderEvent;
import com.example.pharmacyservice.model.Medicine;
import com.example.pharmacyservice.repository.MedicineRepository;
import com.example.pharmacyservice.service.OrderProducerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest
@TestPropertySource(properties = {
        "spring.cache.type=simple",
        "spring.redis.pubsub.enabled=false",
        "spring.redisson.enabled=false"
})
class AsynchronousOrderFlowKafkaTest {

    @MockitoBean
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @MockitoBean
    private KafkaTemplate<String, OrderCreatedEvent> orderCreatedKafkaTemplate;

    @Autowired
    private OrderProducerService orderProducerService;

    @Autowired
    private OrderController orderController;

    @Autowired
    private MedicineRepository medicineRepository;

    private Long sampleMedicineId;

    @BeforeEach
    void setUp() {
        when(kafkaTemplate.send(anyString(), anyString(), any(OrderEvent.class)))
                .thenReturn(new CompletableFuture<>());
        when(orderCreatedKafkaTemplate.send(anyString(), anyString(), any(OrderCreatedEvent.class)))
                .thenReturn(new CompletableFuture<>());

        Medicine med = medicineRepository.findByNameIgnoreCase("Paracetamol 500mg")
                .orElseGet(() -> medicineRepository.save(Medicine.builder()
                        .name("Paracetamol 500mg")
                        .quantity(100)
                        .unitPrice(2500.0)
                        .build()));
        sampleMedicineId = med.getId();
    }

    @Test
    @DisplayName("Bất đồng bộ: Order-Service lưu DB và gửi OrderCreatedEvent vào topic 'order-events', trả về ngay cho client")
    void testPlaceOrder_FiresOrderCreatedEventAndReturnsImmediately() {
        OrderRequest request = OrderRequest.builder()
                .medicineId(sampleMedicineId)
                .quantity(2)
                .customerEmail("customer@pharmacy.vn")
                .customerName("Nguyễn Văn A")
                .build();

        long start = System.currentTimeMillis();
        ResponseEntity<Map<String, Object>> response = orderController.placeOrder(request);
        long duration = System.currentTimeMillis() - start;

        // 1. Phản hồi HTTP 200 cực nhanh cho khách hàng (không bị block bởi trừ kho hay gửi mail)
        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("SUCCESS", response.getBody().get("status"));
        assertEquals("order-events", response.getBody().get("topic"));
        assertTrue(duration < 100, "Luồng đặt hàng phải kết thúc ngay (< 100ms), thực tế: " + duration + " ms");

        // 2. Xác nhận OrderCreatedEvent được phát vào topic 'order-events'
        ArgumentCaptor<OrderCreatedEvent> eventCaptor = ArgumentCaptor.forClass(OrderCreatedEvent.class);
        verify(orderCreatedKafkaTemplate, times(1))
                .send(eq("order-events"), anyString(), eventCaptor.capture());

        OrderCreatedEvent sentEvent = eventCaptor.getValue();
        assertNotNull(sentEvent);
        assertNotNull(sentEvent.getOrderId());
        assertEquals(sampleMedicineId, sentEvent.getMedicineId());
        assertEquals(2, sentEvent.getQuantity());
        assertEquals("customer@pharmacy.vn", sentEvent.getCustomerEmail());
        assertEquals(5000.0, sentEvent.getTotalAmount()); // 2500 * 2 = 5000
    }
}
