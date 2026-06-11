# syntax=docker/dockerfile:1.7

# ── Stage 1: builder ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends     build-essential     gcc     libffi-dev     libssl-dev     curl     git     openssh-client     unzip     && mkdir -p -m 0700 /root/.ssh     && ssh-keyscan github.com >> /root/.ssh/known_hosts     && printf 'Host github.com-kelvin\n  HostName github.com\n  User git\n' > /root/.ssh/config     && rm -rf /var/lib/apt/lists/*

# Install virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:/home/ferraz/.local/share/zed/node/cache/_npx/b01c98eaff59b9fc/node_modules/.bin:/media/ferraz/BACKUP/files/Personal/github/4CastProject/node_modules/.bin:/media/ferraz/BACKUP/files/Personal/github/node_modules/.bin:/media/ferraz/BACKUP/files/Personal/node_modules/.bin:/media/ferraz/BACKUP/files/node_modules/.bin:/media/ferraz/BACKUP/node_modules/.bin:/media/ferraz/node_modules/.bin:/media/node_modules/.bin:/node_modules/.bin:/usr/share/nodejs/@npmcli/run-script/lib/node-gyp-bin:/usr/bin:/home/ferraz/.local/bin:/home/linuxbrew/.linuxbrew/bin:/home/linuxbrew/.linuxbrew/sbin:/home/ferraz/.local/bin:/home/linuxbrew/.linuxbrew/bin:/home/linuxbrew/.linuxbrew/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/snap/bin"

# Install dependencies
COPY pyproject.toml README.md ./
COPY collector/ ./collector/
RUN --mount=type=ssh pip install --upgrade pip     && pip install .

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

ARG AWS_SIGNING_HELPER_VERSION=1.4.0

LABEL org.opencontainers.image.title="4Cast Collector"
LABEL org.opencontainers.image.description="Cloud Evidence Collector for 4Cast platform"

RUN apt-get update && apt-get install -y --no-install-recommends     ca-certificates     curl     && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/4cast/bin     && curl -fsSL "https://rolesanywhere.amazonaws.com/releases/${AWS_SIGNING_HELPER_VERSION}/X86_64/Linux/aws_signing_helper" -o /opt/4cast/bin/aws_signing_helper     && chmod 0755 /opt/4cast/bin/aws_signing_helper

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:/home/ferraz/.local/share/zed/node/cache/_npx/b01c98eaff59b9fc/node_modules/.bin:/media/ferraz/BACKUP/files/Personal/github/4CastProject/node_modules/.bin:/media/ferraz/BACKUP/files/Personal/github/node_modules/.bin:/media/ferraz/BACKUP/files/Personal/node_modules/.bin:/media/ferraz/BACKUP/files/node_modules/.bin:/media/ferraz/BACKUP/node_modules/.bin:/media/ferraz/node_modules/.bin:/media/node_modules/.bin:/node_modules/.bin:/usr/share/nodejs/@npmcli/run-script/lib/node-gyp-bin:/usr/bin:/home/ferraz/.local/bin:/home/linuxbrew/.linuxbrew/bin:/home/linuxbrew/.linuxbrew/sbin:/home/ferraz/.local/bin:/home/linuxbrew/.linuxbrew/bin:/home/linuxbrew/.linuxbrew/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/snap/bin"

RUN groupadd --gid 1001 fourcast     && useradd --uid 1001 --gid fourcast --shell /bin/bash --create-home fourcast

WORKDIR /app
COPY --chown=fourcast:fourcast collector/ ./collector/
COPY --chown=fourcast:fourcast pyproject.toml ./

USER fourcast
ENV HOME=/home/fourcast

ENTRYPOINT ["python", "-m", "collector.main"]
CMD ["--help"]
