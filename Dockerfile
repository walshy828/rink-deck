FROM node:22-alpine
# ffmpeg + yt-dlp power the YouTube -> mp3 import (Settings). node is already here for yt-dlp's JS runtime.
RUN apk add --no-cache ffmpeg python3 curl \
 && curl -fsSL https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
 && chmod a+rx /usr/local/bin/yt-dlp
WORKDIR /app
COPY server.js index.html manifest.webmanifest sw.js icon-192.png icon-512.png icon-maskable-512.png ./
ENV PORT=80 DATA_DIR=/data
VOLUME /data
EXPOSE 80
CMD ["node", "server.js"]
