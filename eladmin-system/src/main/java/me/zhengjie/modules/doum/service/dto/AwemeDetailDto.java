package me.zhengjie.modules.doum.service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * @author liuyi
 * @date 2022/3/10
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AwemeDetailDto {
    private Set<Date> xAxisData;
    private List<Long> digg_count;

    private Set<Date> key1;
    private List<Integer> sales;
}
