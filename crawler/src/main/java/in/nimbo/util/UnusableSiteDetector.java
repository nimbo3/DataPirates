package in.nimbo.util;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnusableSiteDetector {
    private static Logger logger = LoggerFactory.getLogger(UnusableSiteDetector.class);
    private String plainText;

    static {
        try {
            DetectorFactory.loadProfile("profiles");
        } catch (LangDetectException e) {
            logger.error("Failed To Load Languages",e);
        }
    }

    public UnusableSiteDetector(String plainText) {
        this.plainText = plainText;
    }

    public boolean hasAcceptableLanguage() {
        try {
            Detector detector = DetectorFactory.create();
            detector.append(plainText);
            return detector.detect().equals("en");
        } catch (LangDetectException e) {
            logger.error("Failed To Detect Language",e);
            return false;
        }
    }
}
