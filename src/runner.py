import logging
from scrappers.task import ScrapeSeasons, ScrapeMatches, ScrapeEvents
from scrappers.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scrape_seasons():
    seasons_scrapper = ScrapeSeasons(
        network_driver=settings.network_driver,
        tournament_name=settings.tournament_name,
        tournament_url=settings.tournament_url,
        database_client=settings.database_client,
        s3=settings.s3,
        s3_bucket=settings.s3_bucket,
    )
    seasons_scrapper.run()


def scrape_matches():
    season = settings.season
    if season:
        read_seasons = settings.database_client.read_sql(f"SELECT * FROM seasons WHERE id = '{season}'")
    else:
        read_seasons = settings.database_client.read_sql("SELECT * FROM seasons")
    seasons_list = read_seasons.to_dict(orient="records")
    for season in seasons_list:
        season_url = season["url"]
        season_directory = season["season_prefix"]
        season_id = season["id"]
        tournament_directory = season["tournament_prefix"]
        is_current_season = True if season_id == "10317" else False
        matches_scrapper = ScrapeMatches(
            tournament_directory,
            season_url,
            season_directory,
            is_current_season=is_current_season,
            network_driver=settings.network_driver,
            database_client=settings.database_client,
            s3=settings.s3,
            s3_bucket=settings.s3_bucket,
        )
        matches_scrapper.run()


def scrape_events():
    run_context = {
        "scrape_run_id": str(settings.run_id),
        "tournaments": settings.tournament_name,
        "season_id": None,
        "date": None,
        "match_id": None,
        "is_scrapped": None,
        "created_ts": settings.created_ts,
    }
    start_date = settings.start_date
    end_date = settings.end_date
    season = settings.season
    match = settings.match
    if start_date and end_date:
        # Convert YYYY-MM-DD to YYYYMM format for comparison
        start_yyyymm = start_date.replace('-', '')[:6]
        end_yyyymm = end_date.replace('-', '')[:6]
        read_matches = settings.database_client.read_sql(
            f"SELECT * FROM season_matches WHERE date BETWEEN '{start_yyyymm}' AND '{end_yyyymm}'"
        )
    elif season:
        read_matches = settings.database_client.read_sql(
            f"SELECT * FROM season_matches WHERE season_id = {season}"
        )
    elif match:
        read_matches = settings.database_client.read_sql(
            f"SELECT * FROM season_matches WHERE match_id = {match}"
        )
    else:
        read_matches = settings.database_client.read_sql("SELECT * FROM season_matches")
    season_matches = read_matches.to_dict(orient="records")
    for match in season_matches:
        match_id = int(match["match_id"])
        match_url = match["match_url"]
        match_directory = match["match_path"]
        run_context["season_id"] = match["season_id"]
        run_context["date"] = match["date"]
        events_scrapper = ScrapeEvents(
            match_id,
            match_url,
            match_directory,
            run_context,
            network_driver=settings.network_driver,
            database_client=settings.database_client,
            s3=settings.s3,
            s3_bucket=settings.s3_bucket,
        )
        events_scrapper.run()


def main():
    logger.info("=" * 80)
    logger.info("INSTALLED PYTHON PACKAGES")
    logger.info("=" * 80)
    try:
        from importlib.metadata import distributions
        installed_packages = [(d.metadata.get('Name', 'Unknown'), d.version) for d in distributions()]
    except ImportError:
        try:
            import pkg_resources
            installed_packages = [(d.project_name, d.version) for d in pkg_resources.working_set]
        except Exception:
            logger.warning("Could not retrieve installed packages list")
            installed_packages = []
    
    installed_packages.sort()
    for package_name, package_version in installed_packages:
        logger.info(f"  {package_name}=={package_version}")
    logger.info("=" * 80)
    logger.info(f"Total packages: {len(installed_packages)}")
    logger.info("=" * 80)
    scrape_seasons()
    scrape_matches()
    scrape_events()


if __name__ == "__main__":
    main()