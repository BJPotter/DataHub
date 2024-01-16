package com.vasoyn.pojo;

import lombok.Data;
/**
 * Author: Zhi Liu
 * Date: 2024/1/9 10:21
 * Contact: liuzhi0531@gmail.com
 */
@Data
public class ValidatePojo {
    String type;
    String range;
    boolean nullable;
    boolean validateField;
}
