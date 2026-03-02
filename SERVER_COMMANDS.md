# Server Commands Cheat Sheet

Quick reference for common server operations.

> Server: `ubuntu@175.178.23.83`
> Project dir: `/www/daily_stock_analysis`
> Docker Compose file: `docker/docker-compose.yml`

---

## SSH Login

```bash
ssh ubuntu@175.178.23.83
```

## Navigate to Project

```bash
cd /www/daily_stock_analysis
```

---

## Docker Compose Operations

All commands run inside `/www/daily_stock_analysis`.

### Start Services (background)

```bash
sudo docker compose -f docker/docker-compose.yml up -d
```

### Stop Services

```bash
sudo docker compose -f docker/docker-compose.yml down
```

### Restart All Services

```bash
sudo docker compose -f docker/docker-compose.yml restart
```

### Restart Web Server Only

```bash
sudo docker compose -f docker/docker-compose.yml restart server
```

### Restart Analyzer Only

```bash
sudo docker compose -f docker/docker-compose.yml restart analyzer
```

### Rebuild & Restart (after code update)

```bash
sudo docker compose -f docker/docker-compose.yml up -d --build
```

### Rebuild a Single Service

```bash
sudo docker compose -f docker/docker-compose.yml up -d --build server
```

---

## Logs

### View Web Server Logs (follow)

```bash
sudo docker compose -f docker/docker-compose.yml logs -f server
```

### View Analyzer Logs (follow)

```bash
sudo docker compose -f docker/docker-compose.yml logs -f analyzer
```

### View All Logs (follow)

```bash
sudo docker compose -f docker/docker-compose.yml logs -f
```

### View Last 100 Lines

```bash
sudo docker compose -f docker/docker-compose.yml logs --tail=100 server
```

---

## Service Status

### Check Running Containers

```bash
sudo docker compose -f docker/docker-compose.yml ps
```

### Check Container Resource Usage

```bash
sudo docker stats --no-stream
```

---

## Code Update & Deploy

### Pull Latest Code and Rebuild

```bash
cd /www/daily_stock_analysis
sudo git pull origin main
sudo docker compose -f docker/docker-compose.yml up -d --build
```

### Switch to Fork Remote

```bash
cd /www/daily_stock_analysis
sudo git remote set-url origin https://github.com/583175694/daily_stock_analysis.git
sudo git pull origin main
sudo docker compose -f docker/docker-compose.yml up -d --build
```

---

## .env Configuration

### Upload .env from Local Machine

```bash
# Run on local machine (not server)
scp .env ubuntu@175.178.23.83:~/env_tmp && \
ssh ubuntu@175.178.23.83 "sudo mv ~/env_tmp /www/daily_stock_analysis/.env && \
cd /www/daily_stock_analysis && \
sudo docker compose -f docker/docker-compose.yml restart server"
```

### Edit .env on Server

```bash
sudo vim /www/daily_stock_analysis/.env
```

### Restart After .env Change

```bash
sudo docker compose -f docker/docker-compose.yml restart server
```

---

## Sync with Upstream (original repo)

```bash
cd /www/daily_stock_analysis
sudo git fetch upstream
sudo git merge upstream/main
sudo docker compose -f docker/docker-compose.yml up -d --build
```

> Prerequisite: add upstream remote first
> ```bash
> sudo git remote add upstream https://github.com/ZhuLinsen/daily_stock_analysis.git
> ```

---

## Cleanup

### Remove Unused Docker Images

```bash
sudo docker image prune -f
```

### Remove All Unused Docker Resources

```bash
sudo docker system prune -f
```

### Remove Build Cache

```bash
sudo docker builder prune -f
```

---

## Troubleshooting

### Enter Container Shell

```bash
sudo docker compose -f docker/docker-compose.yml exec server bash
```

### Check Port Listening

```bash
sudo ss -tlnp | grep 8000
```

### Check Disk Usage

```bash
df -h
```

### Check Docker Disk Usage

```bash
sudo docker system df
```
