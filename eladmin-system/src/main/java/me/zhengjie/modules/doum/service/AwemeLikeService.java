package me.zhengjie.modules.doum.service;

import me.zhengjie.modules.doum.service.dto.AwemeLikeQueryCriteria;
import me.zhengjie.modules.doum.service.dto.AwemeQueryCriteria;
import org.springframework.data.domain.Pageable;

public interface AwemeLikeService {

    Object list(AwemeLikeQueryCriteria criteria, Pageable pageable);

    Object detail(AwemeLikeQueryCriteria criteria);
}
