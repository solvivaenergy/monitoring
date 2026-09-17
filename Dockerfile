FROM python:3.11-slim
WORKDIR /app
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api/ ./api/
EXPOSE 8000
# Trust Render's proxy headers (X-Forwarded-Proto/For). Without this uvicorn
# only trusts 127.0.0.1, so request.url.scheme is "http" behind the proxy and
# every absolute URL the API builds (the staff-invite return address) comes out
# http://, which Supabase's redirect allow-list then rejects. The container is
# reachable only through Render's proxy, so trusting all forwarders is safe here.
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips=*"]
