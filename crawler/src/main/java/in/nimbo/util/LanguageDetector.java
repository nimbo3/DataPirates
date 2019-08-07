package in.nimbo.util;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LanguageDetector {
    private static Logger logger = LoggerFactory.getLogger(LanguageDetector.class);
    private static Timer langDetectTimer = SharedMetricRegistries.getDefault().timer("language detector");


    public static String detect(String plainText) {
        try (Timer.Context time = langDetectTimer.time()) {
            Detector detector = DetectorFactory.create();
            detector.append(plainText);
            return detector.detect();
        } catch (LangDetectException e) {
            logger.error("Failed To Detect Language", e);
            return "error";
        }
    }
}
