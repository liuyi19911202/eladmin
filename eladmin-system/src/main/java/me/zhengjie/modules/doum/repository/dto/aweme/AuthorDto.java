package me.zhengjie.modules.doum.repository.dto.aweme;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author liuyi
 * @date 2022/3/1
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuthorDto {
    private String nickname;
    private String unique_id;
}
