package in.nimbo.util;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnusableSiteDetector {
    private static Logger logger = LoggerFactory.getLogger(UnusableSiteDetector.class);
    private static Timer acceptableLanguageDetecterTimer = SharedMetricRegistries.getDefault().timer("unusable site detector");
    private String plainText;


    public UnusableSiteDetector(String plainText) {
        this.plainText = plainText;
    }

    public boolean hasAcceptableLanguage() {
        try (Timer.Context time = acceptableLanguageDetecterTimer.time()) {
            Detector detector = DetectorFactory.create();
            detector.append(plainText);
            return detector.detect().equals("en");
        } catch (LangDetectException e) {
            logger.error("Failed To Detect Language",e);
            return false;
        }
    }
}
