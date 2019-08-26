package in.nimbo.util;

import org.apache.commons.codec.digest.DigestUtils;
import java.nio.charset.StandardCharsets;

public class HashCodeGenerator {
    public static String sha2Hash(String link) {
        return DigestUtils.sha256Hex(link);
    }

    public static byte[] md5Hash(String link) {
        return DigestUtils.md5(link);
    }

    public static String md5HashString(String link) {
        return new String(DigestUtils.md5(link), StandardCharsets.UTF_8);
    }
}
