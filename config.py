# config.py
import toml
import glob
import os
from supabase import create_client, Client
from datetime import datetime, timedelta
import unicodedata


class Config:
    def __init__(self, config_path="config.toml"):
        self.config = toml.load(config_path)
        self.supabase = self.init_supabase()
        self.current_time_gmt7 = self.calculate_gmt7_time()

    def get_supabase_config(self):
        supabase_config = self.config.get("supabase", {})
        return supabase_config["url"], supabase_config["key"]

    def get_timezone_offset(self):
        timezone_config = self.config.get("timezone", {})
        return timezone_config.get("offset_hours", 0)

    def init_supabase(self):
        supabase_url, supabase_key = self.get_supabase_config()
        return create_client(supabase_url, supabase_key)

    def calculate_gmt7_time(self):
        timezone_offset_hours = self.get_timezone_offset()
        current_utc_time = datetime.utcnow()
        gmt7_offset = timedelta(hours=timezone_offset_hours)
        return current_utc_time + gmt7_offset

    def get_selenium_config(self):
        selenium_config = self.config.get("selenium", {})
        return selenium_config.get("chrome_options", [])

    def get_paths_config(self):
        paths_config = self.config.get("paths", {})
        return paths_config.get("extension_path", "")

    def get_smartscount(self):
        smartscount_config = self.config.get("smartscount", {})
        return smartscount_config["username"], smartscount_config["password"]


# Utility function to format headers
def format_header(header):
    # Convert to lowercase
    header = header.lower()
    # Replace spaces with underscores
    header = header.replace(" ", "_")
    # Remove Vietnamese characters by decomposing and keeping only ASCII
    header = (
        unicodedata.normalize("NFKD", header).encode("ASCII", "ignore").decode("ASCII")
    )
    return header


# Utility function to get the newest file in a directory
def get_newest_file(directory):
    files = glob.glob(os.path.join(directory, "*"))
    if not files:  # Check if the files list is empty
        return None
    newest_file = max(files, key=os.path.getmtime)
    return newest_file


# Initialize the configuration
config = Config()
