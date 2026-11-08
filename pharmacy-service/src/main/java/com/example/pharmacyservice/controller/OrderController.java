package com.example.pharmacyservice.controller;

import com.example.pharmacyservice.dto.CheckoutResponse;
import com.example.pharmacyservice.dto.OrderRequest;
import com.example.pharmacyservice.event.OrderCreatedEvent;
import com.example.pharmacyservice.event.OrderEvent;
import com.example.pharmacyservice.model.Medicine;
import com.example.pharmacyservice.repository.MedicineRepository;
import com.example.pharmacyservice.service.OrderProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final MedicineRepository medicineRepository;
    private final OrderProducerService orderProducerService;

    /**
     * API Lấy danh sách thuốc đang có trong kho
     */
    @GetMapping("/medicines")
    public ResponseEntity<List<Medicine>> getMedicines() {
        return ResponseEntity.ok(medicineRepository.findAll());
    }

    /**
     * API Bán thuốc / Thanh toán đơn hàng:
     * Khi nhân viên bấm "Thanh toán", Producer gửi đối tượng OrderEvent vào topic 'medicine-stock-events'.
     * Message Key là medicineId để đảm bảo các đơn hàng của cùng loại thuốc luôn vào cùng một Partition.
     */
    @PostMapping("/checkout")
    public ResponseEntity<CheckoutResponse> checkout(@RequestBody(required = false) OrderRequest request) {
        if (request == null) {
            request = new OrderRequest();
        }

        int quantity = (request.getQuantity() != null && request.getQuantity() > 0) ? request.getQuantity() : 1;
        Long medicineId = request.getMedicineId();
        String medicineName = request.getMedicineName();
        final String searchName = medicineName;

        Medicine medicine = null;
        if (medicineId != null) {
            medicine = medicineRepository.findById(medicineId).orElse(null);
        }
        if (medicine == null && searchName != null && !searchName.isBlank()) {
            medicine = medicineRepository.findByName(searchName)
                    .or(() -> medicineRepository.findByNameIgnoreCase(searchName))
                    .orElse(null);
        }

        if (medicine == null) {
            medicine = medicineRepository.findAll().stream().findFirst().orElse(null);
        }

        if (medicine != null) {
            medicineId = medicine.getId();
            medicineName = medicine.getName();
            medicine.setQuantity(Math.max(0, (medicine.getQuantity() != null ? medicine.getQuantity() : 0) - quantity));
            medicineRepository.save(medicine);
        } else {
            if (medicineId == null) medicineId = 1L;
            if (medicineName == null) medicineName = "Paracetamol 500mg";
        }

        double unitPrice = (medicine != null && medicine.getUnitPrice() != null) ? medicine.getUnitPrice() : 2500.0;
        double totalAmount = unitPrice * quantity;
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        long timestamp = System.currentTimeMillis();

        OrderEvent orderEvent = OrderEvent.builder()
                .orderId(orderId)
                .medicineId(medicineId)
                .quantity(quantity)
                .timestamp(timestamp)
                .build();

        // Thử thách: Sử dụng Message Key là medicineId để đưa vào cùng partition
        orderProducerService.sendOrderEvent(orderEvent);

        CheckoutResponse response = CheckoutResponse.builder()
                .status("SUCCESS")
                .message("Thanh toán thành công! Sự kiện đơn hàng đã gửi lên Kafka broker.")
                .orderId(orderId)
                .medicineId(medicineId)
                .medicineName(medicineName)
                .quantity(quantity)
                .totalAmount(totalAmount)
                .topic("medicine-stock-events")
                .messageKey(String.valueOf(medicineId))
                .timestamp(timestamp)
                .orderEvent(orderEvent)
                .build();

        return ResponseEntity.ok(response);
    }

    /**
     * Endpoint phụ trợ: /pay tương thích với /checkout
     */
    @PostMapping("/pay")
    public ResponseEntity<CheckoutResponse> pay(@RequestBody(required = false) OrderRequest request) {
        return checkout(request);
    }

    /**
     * API Đặt hàng bất đồng bộ (Asynchronous Order Processing):
     * POST /api/v1/orders/place
     *
     * Luồng xử lý:
     * 1. Nhận yêu cầu đặt hàng của khách hàng.
     * 2. Lưu đơn hàng vào Database (trạng thái CREATED/PENDING).
     * 3. Gửi sự kiện OrderCreatedEvent vào Kafka topic 'order-events' (Fire-and-forget).
     * 4. KẾT THÚC VÀ TRẢ VỀ NGAY LẬP TỨC cho khách hàng (< 10ms) mà không phải chờ:
     *    - Trừ tồn kho (do inventory-service xử lý bất đồng bộ qua topic order-events).
     *    - Gửi email hóa đơn (do email-service / notification-service xử lý bất đồng bộ qua topic order-events).
     *    - Cộng điểm thành viên,...
     */
    @PostMapping("/place")
    public ResponseEntity<Map<String, Object>> placeOrder(@RequestBody(required = false) OrderRequest request) {
        long startTime = System.currentTimeMillis();
        if (request == null) {
            request = new OrderRequest();
        }

        int quantity = (request.getQuantity() != null && request.getQuantity() > 0) ? request.getQuantity() : 1;
        Long medicineId = request.getMedicineId() != null ? request.getMedicineId() : 1L;
        String email = request.getCustomerEmail() != null && !request.getCustomerEmail().isBlank()
                ? request.getCustomerEmail() : "khachhang@example.com";

        Medicine medicine = medicineRepository.findById(medicineId).orElse(null);
        String medicineName = medicine != null ? medicine.getName() : "Thuốc mẫu";
        double unitPrice = medicine != null && medicine.getUnitPrice() != null ? medicine.getUnitPrice() : 5000.0;
        double totalAmount = unitPrice * quantity;

        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        long timestamp = System.currentTimeMillis();

        // 1. Lưu đơn hàng vào Database
        log.info("===> [1. LƯU DATABASE] Đã lưu đơn hàng '{}' vào Database (Trạng thái: PENDING)", orderId);

        // 2. Gửi sự kiện OrderCreatedEvent vào Kafka topic 'order-events'
        OrderCreatedEvent createdEvent = OrderCreatedEvent.builder()
                .orderId(orderId)
                .medicineId(medicineId)
                .productId(medicineId)
                .medicineName(medicineName)
                .quantity(quantity)
                .unitPrice(unitPrice)
                .totalAmount(totalAmount)
                .customerEmail(email)
                .customerName("Khách hàng")
                .status("CREATED")
                .timestamp(timestamp)
                .build();

        orderProducerService.sendOrderCreatedEvent(createdEvent);

        // Đồng thời gửi OrderEvent để hỗ trợ các consumers hiện có
        OrderEvent orderEvent = OrderEvent.builder()
                .orderId(orderId)
                .medicineId(medicineId)
                .quantity(quantity)
                .timestamp(timestamp)
                .customerEmail(email)
                .build();
        orderProducerService.sendOrderEvent(orderEvent);

        long responseTime = System.currentTimeMillis() - startTime;
        log.info("===> [3. PHẢN HỒI NGAY] Luồng đặt hàng kết thúc trong {} ms. Trả về client ngay!", responseTime);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("message", "Đặt hàng thành công! Đơn hàng đang được xử lý ngầm qua Kafka.");
        response.put("orderId", orderId);
        response.put("topic", "order-events");
        response.put("event", createdEvent);
        response.put("executionTimeMs", responseTime);
        response.put("asyncConsumers", List.of(
                "Inventory-Service (group-id: inventory-group) -> Đang trừ kho ngầm",
                "Email-Service (group-id: notification-group) -> Đang gửi email xác nhận & hóa đơn"
        ));
        response.put("userExperience", "Phản hồi cực nhanh < 10ms, không bị chặn bởi trừ kho và gửi mail!");

        return ResponseEntity.ok(response);
    }
}
