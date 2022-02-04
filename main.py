import sys
import os
import re
import json
import sqlite3

from pymediainfo import MediaInfo

try:
    conn = sqlite3.connect("media_weeder.db")
except Error as e:
    print("[!] Error opening DB")
    print(e)
    sys.exit()

class Scanner(object):
    def __init__(self, scanning_path, ignore):
        self.scanning_path = scanning_path
        self.ignore = re.compile(ignore)
        self.db = None

        self.setup_db()

        

    def get_file_info(self, file_path):
        info_dict = dict()
        try:
            media_info = MediaInfo.parse(file_path)

            general_track = media_info.general_tracks[0]
            video_track = media_info.video_tracks[0]

            info_dict["file_name"] = general_track.file_name
            info_dict["complete_name"] = general_track.complete_name
            info_dict["file_extension"] = general_track.file_extension
            info_dict["file_size"] = general_track.file_size
            info_dict["human_file_size"] = general_track.other_file_size[0].split()[0]

            info_dict["height"] = video_track.height
            info_dict["width"] = video_track.width
        
        except Exception as E:
            print("[!] Error with {file_path}".format(file_path=file_path))
            error_dict = {
                    'file_path': file_path,
                    'error_type': type(E),
                    'error_text': str(E)
                    }
            self.save_error_to_db(error_dict)
            return {}
    
        return info_dict

    def start_scanning(self):
        for directory, _, files in os.walk(self.scanning_path):
            for f in files:
                file_path = os.path.join(directory, f)
                if not re.match(self.ignore, file_path):
                    file_info = self.get_file_info(file_path)
                    if file_info:
                        print("[*] Scanned: {file_info[complete_name]}".format(file_info=file_info))
                        self.save_to_db(file_info)



    def save_to_db(self, file_info):
        insert_string = """
            INSERT INTO media_info 
                (FILE_NAME, COMPLETE_NAME, FILE_EXTENSION, FILE_SIZE, HUMAN_FILE_SIZE, HEIGHT, WIDTH)
            VALUES
                (
                    "{i[file_name]}", 
                    "{i[complete_name]}", 
                    "{i[file_extension]}", 
                    {i[file_size]},
                    {i[human_file_size]}, 
                    {i[height]}, 
                    {i[width]}
                );
        """.format(i=file_info)

        self.db.execute(insert_string)
        self.db.commit()

    def save_error_to_db(self, error_info):
        insert_string = """
            INSERT INTO errors
                (FILE_PATH, ERROR_TYPE, ERROR_TEXT)
            VALUES
                (
                    "{e[file_path]}",
                    "{e[error_type]}",
                    "{e[error_text]}"
                );
        """.format(e=error_info)

        self.db.execute(insert_string)
        self.db.commit()
    
    def setup_db(self):
        db_path = 'media_weeder.db'
        try:
            self.db = sqlite3.connect(db_path)
        except Error as e:
            print("[!] Error opening DB")
            print(e)
            sys.exit()
        print("[*] DB: {db_path}".format(db_path=db_path))

        create_info_table_string = """
            CREATE TABLE IF NOT EXISTS media_info 
            (
                ID INT AUTO_INCREMENT,
                FILE_NAME TEXT NOT NULL,
                COMPLETE_NAME TEXT NOT NULL,
                FILE_EXTENSION TEXT NOT NULL,
                FILE_SIZE INT NOT NULL,
                HUMAN_FILE_SIZE INT NOT NULL,
                HEIGHT INT NOT NULL,
                WIDTH INT NOT NULL
            );
        """

        create_error_table_string = """
            CREATE TABLE IF NOT EXISTS errors 
            (
                ID INT AUTO INCREMENT,
                FILE_PATH TEXT NOT NULL,
                ERROR_TYPE TEXT,
                ERROR_TEXT TEXT
            );
        """
        self.db.execute(create_info_table_string)
        self.db.execute(create_error_table_string)
        


def main():
    scanning_path = sys.argv[1]
    ignore = sys.argv[2]
    scanner = Scanner(scanning_path, ignore=ignore)
    scanner.start_scanning()


if __name__ == '__main__':
    main()
