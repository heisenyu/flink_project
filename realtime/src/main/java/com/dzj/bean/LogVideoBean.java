package com.dzj.bean;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LogVideoBean {
    String userCode;

    Integer level;

//    String eventCode;

    String videoId;

    Integer logCount;

    Integer duration;

    Integer videoLength;

    Integer ruleLength;

    Double videoPercent;

    Double rulePercent;

    String taskCode;

    String ruleCode;
}
