#!/bin/sh
find . -path '*/migrations/*.py*' -not -path './env/*' -not -name '__init__.py' -delete 
rm db.sqlite3
python3 manage.py makemigrations
python3 manage.py migrate
python3 manage.py collectstatic
python3 manage.py createsuperuser --username admin
