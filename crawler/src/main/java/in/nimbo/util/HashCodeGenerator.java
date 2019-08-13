package in.nimbo.util;

import org.apache.commons.codec.digest.DigestUtils;

public class HashCodeGenerator {
    public String sha2Hash(String link) {
        return DigestUtils.sha256Hex(link);
    }
}
