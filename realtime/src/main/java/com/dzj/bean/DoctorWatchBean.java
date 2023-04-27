package com.dzj.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Author: zhangly
 * Date: 2023/4/26 14:41
 * FileName: DoctorWatchBean
 * Description: 医生观看记录bean
 */

@EqualsAndHashCode(callSuper = false)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DoctorWatchBean {
    String userId;
    Integer doctorLevel;
    String resourceId;
    Integer count;
    Integer duration;
    Double percentage;


}
