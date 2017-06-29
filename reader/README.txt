mysql -u root --host localhost reddit < ../database/create-database.sql -p
python reader.py /r/photoshopbattles
python reader.py --get-comments /output/path