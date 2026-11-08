package com.example.pharmacyservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Sự kiện OrderCreatedEvent được Order-Service phát lên Kafka topic 'order-events'
 * ngay sau khi đơn hàng được lưu vào Database.
 *
 * Hai consumers độc lập:
 * 1. Inventory-Service (group-id: inventory-group) -> Trừ kho
 * 2. Email-Service (group-id: email-group)        -> Gửi email xác nhận & hóa đơn
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderCreatedEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String orderId;
    private Long medicineId;
    private Long productId;
    private String medicineName;
    private Integer quantity;
    private Double unitPrice;
    private Double totalAmount;
    private String customerEmail;
    private String customerName;
    private String status;
    private Long timestamp;
}
