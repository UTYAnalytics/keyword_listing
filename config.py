# config.py
import toml
from supabase import create_client, Client
from datetime import datetime, timedelta

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
        return selenium_config.get("username"), selenium_config.get("password"), selenium_config.get("chrome_options", [])

    def get_paths_config(self):
        paths_config = self.config.get("paths", {})
        return paths_config.get("extension_path", "")

# Initialize the configuration
config = Config()
