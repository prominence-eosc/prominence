#!/bin/sh
find . -path '*/migrations/*.py*' -not -name '__init__.py' -delete
rm db.sqlite3
python3 manage.py makemigrations custom_user
python3 manage.py makemigrations web
python3 manage.py migrate
python3 manage.py createsuperuser --username admin
