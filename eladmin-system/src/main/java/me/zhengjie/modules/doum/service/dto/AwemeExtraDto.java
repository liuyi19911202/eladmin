package me.zhengjie.modules.doum.service.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AwemeExtraDto {

    // ----------------商品信息------------

    private String promotion_id;
    private String product_id;
    private String title;
    private Double price;
    private Integer sales;

    // ----------------商品信息------------

}
