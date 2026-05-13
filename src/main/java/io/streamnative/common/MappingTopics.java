package io.streamnative.common;

import java.util.List;
import lombok.Data;

@Data
public class MappingTopics {
    public List<String> inputTopics;
    public String outputTopic;
}