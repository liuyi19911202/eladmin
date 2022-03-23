package me.zhengjie.modules.doum.repository.dto.aweme;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author liuyi
 * @date 2022/3/1
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AwemeDetailDto {
    private AwemeDdetailDataDto data;
    private Integer code;
    private String success;
}
