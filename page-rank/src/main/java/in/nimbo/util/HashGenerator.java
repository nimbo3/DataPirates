package in.nimbo.util;

import org.apache.commons.codec.digest.DigestUtils;

import java.nio.charset.StandardCharsets;

public class HashGenerator {

    public static String md5HashString(String str) {
        return new String(DigestUtils.md5(str), StandardCharsets.UTF_8);
    }

    public static String md5HashString(byte[] bytes) {
        return new String(DigestUtils.md5(bytes), StandardCharsets.UTF_8);
    }

    public static byte[] md5HashBytes(String str) {
        return DigestUtils.md5(str);
    }

    public static byte[] md5HashBytes(byte[] bytes) {
        return DigestUtils.md5(bytes);
    }

}
