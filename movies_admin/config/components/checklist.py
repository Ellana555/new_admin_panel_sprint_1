# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get("SECRET_KEY")
# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.environ.get("DEBUG", False) == True

ALLOWED_HOSTS = os.environ.get("ALLOWED_HOSTS").split(',')

# INTERNAL_IPS = [
#    '127.0.0.1',
# ]
