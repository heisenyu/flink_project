package com.dzj.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LogBean {
    String deviceId;

    String userCode;

    String eventCode;

    String eventType;

    Long ts;

    String resourceId;
}
