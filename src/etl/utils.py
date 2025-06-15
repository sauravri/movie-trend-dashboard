# separate utility file with helper functions for:

#    - fallback sanitization

#    - logging configuration

#    - transaction handlers (commit, rollback)

import logging

# Configure logging
logging.basicConfig(
    filename="load_data.log",
    level=logging.INFO,
    format="[ %(levelname)s ] %(asctime)s - %(message)s",
)


def fallback_int(value, fallback=0):
    """Safely converts a value to int, fallback if invalid or missing."""
    try:
        return int(value)
    except (ValueError, TypeError):
        logging.error(f"Invalid int value: {value}. Using fallback {fallback}.")
        return fallback


def fallback_float(value, fallback=0.0):
    """Safely converts a value to float, fallback if invalid or missing."""
    try:
        return float(value)
    except (ValueError, TypeError):
        logging.error(f"Invalid float value: {value}. Using fallback {fallback}.")
        return fallback


def fallback_str(value, fallback="Unknown"):
    """Safely converts a value to str, fallback if invalid or missing."""
    if value:
        return str(value)
    logging.error(f"Invalid or missing str. Using fallback {fallback}.")
    return fallback


def fallback_year(release_date, fallback=None):
    """Safely parses year from release_date (format YYYY-MM-DD), fallback if invalid or missing."""
    try:
        return int(release_date.split("-")[0]) if release_date else fallback
    except (ValueError, IndexError, TypeError):
        logging.error(
            f"Invalid release date: {release_date}. Using fallback {fallback}."
        )
        return fallback


def fallback_genre_ids(genre_ids, fallback=None):
    """Safely parses genre IDs; fallback to empty if invalid or missing."""
    if genre_ids and isinstance(genre_ids, list):
        return genre_ids
    logging.error("Invalid or missing genre IDs.")
    return fallback or []
