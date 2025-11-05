from src import config

# For now just stubs — we'll add real logic later

def check_monthly_cap(current_calls: int):
    if current_calls >= config.MONTHLY_CAP:
        raise Exception("Monthly API call cap reached")

def rate_limit():
    # placeholder — we'll add proper rate limiting soon
    pass
