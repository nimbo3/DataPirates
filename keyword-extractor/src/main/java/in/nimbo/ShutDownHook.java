package in.nimbo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ShutDownHook extends Thread{
    private static final Logger logger = LoggerFactory.getLogger(ShutDownHook.class);
    private ElasticDao elasticDao;
    ShutDownHook(ElasticDao elasticDao){
        this.elasticDao = elasticDao;
    }
    @Override
    public void run() {
        try {
            elasticDao.close();
        } catch (IOException e) {
            logger.error("can't stop elastic connection", e);
        }
    }
}
