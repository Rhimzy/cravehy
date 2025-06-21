import re
import os
import logging

# --- Configuration ---
LOG_FILE_PATH = "scrapfetchlogs.log"
FAILED_PIDS_FILE_PATH = "failed_pids_from_log.txt" # Output file for the extracted PIDs

# Set up a basic logger for this script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_failed_pids(log_file_path):
    """
    Reads a log file and extracts unique product IDs (PIDs) from lines
    that specifically contain a '403 Client Error: Forbidden' for a PDP URL.

    Args:
        log_file_path (str): The path to the log file (e.g., 'scraper_log.log').

    Returns:
        set: A set of unique PIDs that failed to be fetched.
    """
    failed_pids = set()
    
    # Regex to specifically match the error lines and capture the PID from "(ID: XXXXXX)"
    # It looks for "Error fetching PDP URL", then any characters, then "(ID: " followed by digits (captured group 1),
    # then "):", then any characters, then "403 Client Error: Forbidden".
    pid_pattern = re.compile(r"Error fetching PDP URL .* \(ID: (\d+)\): .*403 Client Error: Forbidden")

    if not os.path.exists(log_file_path):
        logging.error(f"Log file not found: {log_file_path}")
        return set()

    logging.info(f"Reading log file: {log_file_path}")
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                match = pid_pattern.search(line)
                if match:
                    pid = match.group(1) # Group 1 contains the digits (PID)
                    failed_pids.add(pid)
    except Exception as e:
        logging.error(f"An error occurred while reading the log file: {e}")
        return set()

    logging.info(f"Finished reading log. Found {len(failed_pids)} unique failed PIDs.")
    return failed_pids

def save_pids_to_file(pids_set, output_file_path):
    """
    Saves a set of unique PIDs to a text file, with one PID per line.

    Args:
        pids_set (set): A set of unique PIDs.
        output_file_path (str): The path where the PIDs will be saved.
    """
    if not pids_set:
        logging.info("No PIDs to save to file.")
        return

    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            # Convert set to a list and sort it for consistent output order
            for pid in sorted(list(pids_set)):
                f.write(f"{pid}\n")
        logging.info(f"Successfully saved {len(pids_set)} PIDs to {output_file_path}")
    except Exception as e:
        logging.error(f"An error occurred while saving PIDs to file: {e}")

if __name__ == "__main__":
    logging.info("Starting PID extraction from log file.")
    
    # Execute the extraction process
    extracted_pids = extract_failed_pids(LOG_FILE_PATH)

    # Save the extracted PIDs to a file
    save_pids_to_file(extracted_pids, FAILED_PIDS_FILE_PATH)
    
    logging.info("PID extraction script finished.")