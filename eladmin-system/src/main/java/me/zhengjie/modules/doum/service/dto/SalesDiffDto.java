package me.zhengjie.modules.doum.service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author liuyi
 * @date 2022/3/5
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SalesDiffDto {
    private String product_id;
    private String title;
    private Integer sales;
    private Integer sales_diff;
}
