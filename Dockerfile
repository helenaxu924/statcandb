FROM python:3.11.4-slim-bookworm AS source_cache

WORKDIR /app
COPY . /app/
RUN find . -type f | grep -vE '*\.py$|README.*|pyproject.*|poetry.*|config.*|py.typed|db.db' | xargs rm -rf \
    && rm -rf tests \
    && find . -type d -empty | xargs rm -rf

FROM python:3.11.4-slim-bookworm

# Set timezone
RUN echo "US/Eastern" > /etc/timezone && \
    dpkg-reconfigure --frontend noninteractive tzdata

# Install packages while still root
RUN apt-get update \
    && apt-get install -y curl git --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Create a default user and install rest of dependencies for user
RUN groupadd --system automation && \
    useradd --system --create-home --gid automation --groups audio,video automation && \
    mkdir --parents /home/automation/app && \
    chown --recursive automation:automation /home/automation

USER automation
WORKDIR /home/automation/app

# Install poetry
RUN curl -sSL https://install.python-poetry.org > install-poetry.py \
    && python3 install-poetry.py \
    && rm -f install-poetry.py

ENV PATH="/home/automation/.local/bin:$PATH"

# Install dependencies without copying rest of code over to avoid extra time on build
COPY pyproject.toml poetry.lock /home/automation/app/
RUN poetry config virtualenvs.in-project true \
    && poetry install --no-root --only main

COPY --from=source_cache --chown=automation:automation /app /home/automation/app/
RUN poetry install --only main

CMD ["poetry", "run", "statcandb"]
