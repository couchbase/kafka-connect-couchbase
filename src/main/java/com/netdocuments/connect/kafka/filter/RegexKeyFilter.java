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
    private static final String SHARESPACE_CONFIG = "couchbase.event.filter.sharespace.filter";

    private Pattern pattern;
    private boolean filterSharespaces;

    @Override
    public void init(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(getConfigDef(), props);
        String regex = config.getString(REGEX_CONFIG);
        boolean caseInsensitive = config.getBoolean(REGEX_CASE_INSENSITIVE_CONFIG);
        filterSharespaces = config.getBoolean(SHARESPACE_CONFIG);

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
        if (filterSharespaces && isShareSpace(key)) {
            LOGGER.debug("Key '{}' is a sharespace, skipping", key);
            return false;
        }
        boolean matches = pattern.matcher(key).matches();
        LOGGER.debug("Key '{}' {} regex filter", key, matches ? "passed" : "did not pass");
        return matches;
    }

    private static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(REGEX_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Regular expression to match document keys")
                .define(REGEX_CASE_INSENSITIVE_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                        "Whether the regex matching should be case-insensitive")
                .define(SHARESPACE_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                        "Whether to filter out sharespace events");
    }

    /**
     * Fetches the envelope type of the given url. Returns true only if it's a
     * ShareSpace. A sharespace URL can be spotted by looking at the first and
     * second character after the last slash. It is not a sharespace if the first
     * character is a ~ or the first character is a ^ and the second character is
     * not a D. Any other case it is a sharespace.
     *
     * @param url The URL to analyze.
     * @return Returns true if the envelope type of the given url is a ShareSpace,
     *         false otherwise.
     */
    static boolean isShareSpace(String url) {
        url = modifyKey(url);
        if (url != null && !url.isEmpty()) {
            if (url.indexOf(':') >= 0) {
                url = url.replace(':', '/'); // transform REST API ID into an envUrl
            }
            int pos = url.lastIndexOf('/');
            if (pos >= 0 && pos + 2 <= url.length()) {
                char c1 = url.charAt(pos + 1);
                switch (c1) {
                    case '~':
                        return false;
                    case '^':
                        if (pos + 2 < url.length()) {
                            char c2 = Character.toLowerCase(url.charAt(pos + 2));
                            return c2 == 'd'; // ShareSpace (NetBinder, former NetEnvelope)
                        }
                        return false;
                }
            }
        }
        return true; // Default case is still ShareSpace (NetBinder)
    }

    /**
     * Modifies the original key by adding '/' after each of the next four letters
     * after the existing '/'. Keys take the form of:
     * MDucot5/qbti~240924115010829
     * HDucot5/qbti~240924115010829
     *
     * @param originalKey The original document key
     * @return The modified key with additional '/' characters
     */
    static String modifyKey(String originalKey) {
        int firstSlashIndex = originalKey.indexOf('/');
        if (firstSlashIndex == -1 || firstSlashIndex + 5 > originalKey.length()) {
            return originalKey;
        }

        StringBuilder modifiedKey = new StringBuilder(originalKey);
        for (int i = 1; i <= 4; i++) {
            modifiedKey.insert(firstSlashIndex + i * 2, '/');
        }
        return modifiedKey.toString();
    }
}
