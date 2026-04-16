import logging, time
from scrappers.task import ScrapeSeasons, ScrapeMatches, ScrapeEvents
from scrappers.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RUN = settings.run_id[:8]

FORCE_SEASONS = settings.force_refresh_seasons
FORCE_MATCHES = settings.force_refresh_matches or FORCE_SEASONS
FORCE_EVENTS = settings.force_refresh_events or FORCE_MATCHES


def scrape_seasons():
    ctx = f"[run={RUN} step=seasons]"
    force = FORCE_SEASONS
    logger.info(f"{ctx} Starting season scrape for league={settings.tournament_name} force_refresh={force}")
    seasons_scrapper = ScrapeSeasons(
        network_driver=settings.network_driver,
        tournament_name=settings.tournament_name,
        tournament_url=settings.tournament_url,
        s3=settings.s3,
        s3_bucket=settings.s3_bucket,
        run_id=settings.run_id,
    )
    t0 = time.time()
    seasons_scrapper.run(force=force)
    logger.info(f"{ctx} Done elapsed={time.time() - t0:.1f}s")


def scrape_matches():
    ctx = f"[run={RUN} step=matches]"
    force = FORCE_MATCHES
    logger.info(f"{ctx} Starting match scrape force_refresh={force}")
    season = settings.season
    if season:
        read_seasons = settings.database_client.read_sql(f"SELECT * FROM seasons WHERE id = '{season}'")
    else:
        read_seasons = settings.database_client.read_sql("SELECT * FROM seasons")
    seasons_list = read_seasons.to_dict(orient="records")
    total_seasons = len(seasons_list)
    logger.info(f"{ctx} Found total_seasons={total_seasons}")

    max_season_id = str(max(int(s["id"]) for s in seasons_list)) if seasons_list else None

    t0 = time.time()
    saved = 0
    skipped = 0
    for i, season in enumerate(seasons_list, 1):
        season_url = season["url"]
        season_directory = season["season_prefix"]
        season_id = season["id"]
        tournament_directory = season["tournament_prefix"]
        is_current_season = (str(season_id) == max_season_id)
        logger.info(f"{ctx} Processing season {i}/{total_seasons} season={season_id} current={is_current_season}")
        matches_scrapper = ScrapeMatches(
            tournament_directory,
            season_url,
            season_directory,
            is_current_season=is_current_season,
            network_driver=settings.network_driver,
            s3=settings.s3,
            s3_bucket=settings.s3_bucket,
            run_id=settings.run_id,
        )
        result = matches_scrapper.run(force=force)
        if result == "skipped":
            skipped += 1
        else:
            saved += 1
    logger.info(f"{ctx} Done elapsed={time.time() - t0:.1f}s saved={saved} skipped={skipped}")


def scrape_events():
    ctx = f"[run={RUN} step=events]"
    force = FORCE_EVENTS
    logger.info(f"{ctx} Starting event scrape force_refresh={force}")
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
    cols = "DISTINCT match_id, match_path, match_url, date, season_id"
    if start_date and end_date:
        start_yyyymm = start_date.replace('-', '')[:6]
        end_yyyymm = end_date.replace('-', '')[:6]
        read_matches = settings.database_client.read_sql(
            f"SELECT {cols} FROM season_matches WHERE date BETWEEN '{start_yyyymm}' AND '{end_yyyymm}'"
        )
    elif season:
        read_matches = settings.database_client.read_sql(
            f"SELECT {cols} FROM season_matches WHERE season_id = {season}"
        )
    elif match:
        read_matches = settings.database_client.read_sql(
            f"SELECT {cols} FROM season_matches WHERE match_id = {match}"
        )
    else:
        read_matches = settings.database_client.read_sql(f"SELECT {cols} FROM season_matches")
    season_matches = read_matches.to_dict(orient="records")
    total_matches = len(season_matches)
    logger.info(f"{ctx} Found total_matches={total_matches}")

    starttime_df = settings.database_client.read_sql("SELECT id, starttime FROM monthly_matches")
    starttime_lookup = dict(zip(starttime_df["id"], starttime_df["starttime"]))
    logger.info(f"{ctx} Loaded starttime lookup for {len(starttime_lookup)} matches")

    t0 = time.time()
    saved = 0
    skipped = 0
    failed = 0
    for i, match in enumerate(season_matches, 1):
        match_id = int(match["match_id"])
        match_url = match["match_url"]
        match_directory = match["match_path"]
        run_context["season_id"] = match["season_id"]
        run_context["date"] = match["date"]
        try:
            events_scrapper = ScrapeEvents(
                match_id,
                match_url,
                match_directory,
                run_context,
                match_starttime=starttime_lookup.get(match_id),
                network_driver=settings.network_driver,
                s3=settings.s3,
                s3_bucket=settings.s3_bucket,
            )
            result = events_scrapper.run(force=force)
            if result == "skipped":
                skipped += 1
            else:
                saved += 1
        except Exception as e:
            failed += 1
            logger.error(
                f"{ctx} [season={match['season_id']} match={match_id}] "
                f"Failed error=\"{type(e).__name__}: {e}\""
            )
        if i % 50 == 0 or i == total_matches:
            logger.info(f"{ctx} Progress: {i}/{total_matches} saved={saved} skipped={skipped} failed={failed}")
    logger.info(f"{ctx} Done elapsed={time.time() - t0:.1f}s saved={saved} skipped={skipped} failed={failed}")


def main():
    ctx = f"[run={RUN}]"
    logger.info(f"{ctx} WS Scrapper starting")
    logger.info(f"{ctx}   tournament={settings.tournament_name} type={settings.scrapping_type.value}")
    logger.info(f"{ctx}   season={settings.season or 'all'} match={settings.match or 'all'}")
    logger.info(f"{ctx}   date_range={settings.start_date or '-'} -> {settings.end_date or '-'}")
    logger.info(f"{ctx}   driver={settings.driver_type.value}")
    logger.info(
        f"{ctx}   force_refresh: seasons={FORCE_SEASONS} "
        f"matches={FORCE_MATCHES} events={FORCE_EVENTS}"
    )

    logger.debug("INSTALLED PYTHON PACKAGES")
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
        logger.debug(f"  {package_name}=={package_version}")
    logger.debug(f"Total packages: {len(installed_packages)}")

    t_total = time.time()
    scrape_seasons()
    scrape_matches()
    scrape_events()
    logger.info(f"{ctx} Pipeline complete elapsed={time.time() - t_total:.1f}s")


if __name__ == "__main__":
    main()
