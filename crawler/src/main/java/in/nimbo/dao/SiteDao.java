package in.nimbo.dao;

import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;

import java.io.Closeable;

public interface SiteDao extends Closeable {
    void insert(Site site) throws SiteDaoException;

    void delete(String url) throws SiteDaoException;
}
