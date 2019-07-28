package in.nimbo.database.dao;

import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;

public interface SiteDao {
    void insert(Site site) throws SiteDaoException;
    void delete(String url) throws SiteDaoException;
}
