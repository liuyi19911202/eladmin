package me.zhengjie.modules.quartz.task;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import me.zhengjie.modules.doum.service.dto.AwemeExtraDto;

import java.util.List;

/**
 * @author liuyi
 * @date 2022/4/5
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisDto {
    private String[] url_list;
    private List<AwemeExtraDto> extra;
}
