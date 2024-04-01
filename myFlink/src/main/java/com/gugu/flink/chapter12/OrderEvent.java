package com.gugu.flink.chapter12;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrderEvent {
    private String orderId;
    private String eventType;
    private String userId;
    private Long timestamp;
}
