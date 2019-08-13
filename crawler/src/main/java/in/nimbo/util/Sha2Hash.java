package in.nimbo.util;

import org.apache.commons.codec.digest.DigestUtils;

public class Sha2Hash {
    public String hash(String link) {
        return DigestUtils.sha256Hex(link);
    }
}
