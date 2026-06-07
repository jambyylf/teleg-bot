FROM python:3.11-slim

# Deno — yt-dlp signature/n-challenge шешуші (толық YouTube сапасы: 1440p/4K).
# Ресми deno образынан тек бинарьді көшіреміз (PATH-та /usr/local/bin).
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno

# FFmpeg + Playwright Chromium үшін жүйелік кітапханалар
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxext6 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright Chromium браузерін жүктеу
RUN playwright install chromium

COPY . .

CMD ["python", "bot.py"]
