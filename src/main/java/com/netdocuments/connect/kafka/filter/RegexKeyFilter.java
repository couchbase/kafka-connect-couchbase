package com.netdocuments.connect.kafka.filter;

import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;

public class RegexKeyFilter implements Filter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegexKeyFilter.class);

    private static final String REGEX_CONFIG = "couchbase.event.filter.regex";
    private static final String REGEX_CASE_INSENSITIVE_CONFIG = "couchbase.event.filter.regex.case_insensitive";

    private Pattern pattern;

    @Override
    public void init(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(getConfigDef(), props);
        String regex = config.getString(REGEX_CONFIG);
        boolean caseInsensitive = config.getBoolean(REGEX_CASE_INSENSITIVE_CONFIG);

        if (regex == null || regex.isEmpty()) {
            throw new ConfigException("RegexKeyFilter requires a non-empty regex");
        }

        try {
            int flags = caseInsensitive ? Pattern.CASE_INSENSITIVE : 0;
            this.pattern = Pattern.compile(regex, flags);
        } catch (Exception e) {
            throw new ConfigException("Invalid regex for RegexKeyFilter", e);
        }

        LOGGER.info("Initialized RegexKeyFilter with pattern: {} (case insensitive: {})", regex, caseInsensitive);
    }

    @Override
    public boolean pass(DocumentEvent event) {
        String key = event.key();
        boolean matches = pattern.matcher(key).matches();
        LOGGER.debug("Key '{}' {} regex filter", key, matches ? "passed" : "did not pass");
        return matches;
    }

    private static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(REGEX_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Regular expression to match document keys")
                .define(REGEX_CASE_INSENSITIVE_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                        "Whether the regex matching should be case-insensitive");
    }
}
