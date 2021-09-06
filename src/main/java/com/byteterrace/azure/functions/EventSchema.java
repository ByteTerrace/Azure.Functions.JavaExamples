package com.byteterrace.azure.functions;

import java.util.Date;
import java.util.Map;

public final class EventSchema {
    public Map<String, Object> data;
    public String dataVersion;
    public String eventType;
    public Date eventTime;
    public String id;
    public String metadataVersion;
    public String topic;
    public String subject;
}
