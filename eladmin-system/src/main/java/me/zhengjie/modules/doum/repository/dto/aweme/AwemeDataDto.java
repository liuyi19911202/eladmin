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
public class AwemeDataDto {
    private List<AwemeListObjectDto> aweme_list;
    private Long has_more;
    private Long max_cursor;
    private Integer status_code;
}
