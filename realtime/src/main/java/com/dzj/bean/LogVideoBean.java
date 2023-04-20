package com.dzj.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class LogVideoBean extends LogBean {
    String deviceId;

    String userCode;

    String eventCode;

    String ts;

    String videoId;
}
