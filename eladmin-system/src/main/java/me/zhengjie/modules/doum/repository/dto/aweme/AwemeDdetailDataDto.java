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
public class AwemeDdetailDataDto {
    private AwemeListObjectDto aweme_detail;
    private Integer status_code;
}
