import os
from pathlib import Path
from dotenv import load_dotenv
from split_settings.tools import include


load_dotenv()


# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
include(
    "components/checklist.py",
)


# Application definition
include(
    "components/app_def.py",
)


# Database
include(
    "components/database.py",
)


# Password validation
include(
    "components/password.py",
)


# Internationalization
include(
    "components/translations.py",
)

# Static files (CSS, JavaScript, Images)
include(
    "components/static-files.py",
)


# Default primary key field type
include(
    "components/auto_field.py",
)
