# Gunicorn configuration for Finpolaris Stock-Trace-Server
# Memory budget: ~1GB total (MySQL 200MB + nginx 20MB + omnisight 150MB + this)

bind = "127.0.0.1:5000"
workers = 1              # 1 worker to stay within memory; increase to 2 only if free -m shows >300MB free
worker_class = "sync"
threads = 2              # 2 threads per worker for concurrent API requests
timeout = 120            # Tushare API / TableStore calls can be slow
max_requests = 1000      # prevent slow memory leaks from long-running workers
max_requests_jitter = 100
preload_app = False      # bind port immediately; lazy init on first request
loglevel = "info"
accesslog = "-"          # stdout → systemd journal
errorlog = "-"           # stdout → systemd journal
