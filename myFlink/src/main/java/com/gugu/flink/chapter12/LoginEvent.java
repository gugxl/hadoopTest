package com.gugu.flink.chapter12;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    private String userId;
    private String ipAddress;
    private String eventType;
    private Long timestamp;
}
