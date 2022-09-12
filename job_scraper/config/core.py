from diot import Diot
import yaml
from yaml.loader import SafeLoader
from pathlib import Path
import job_scraper

PACKAGE_ROOT = Path(job_scraper.__file__).resolve().parent
CONFIG_PATH = PACKAGE_ROOT / "config/config.yml"


with open(CONFIG_PATH) as f:
    data = yaml.load(f, Loader=SafeLoader)

config = Diot(data)
print(config)