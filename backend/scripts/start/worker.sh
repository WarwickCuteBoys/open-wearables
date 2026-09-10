#!/bin/bash
set -e -x

exec uv run celery -A app.main:celery_app worker --loglevel=info --pool=threads -Q default,sdk_sync,garmin_sync,webhook_sync
