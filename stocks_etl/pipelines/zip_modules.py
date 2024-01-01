from pathlib import Path
import os
import zipfile  
from stocks_etl.utils.helper_functions import configured_logger

logger = configured_logger("zip-modules")

def zipit(folders_list, zip_filename):
    """Function that zips a list of folders
    Parameters
    ----------
        folders_list (list): List of folders to be zipped.
        zip_filename (str): Filename for the zip file.
    """
    logger.info("Ziping python modules")
    
    # Create zip_file instance
    zip_file = zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED)

    # Zip the folders
    for folder in folders_list:
        for dirpath, dirnames, filenames in os.walk(folder):
            for filename in filenames:
                zip_file.write(
                    os.path.join(dirpath, filename),
                    os.path.relpath(os.path.join(dirpath, filename), os.path.join(folders_list[0], '../..')))

    zip_file.close()

def zip_modules_pipeline():
    """Pipeline to zip the modules folders and __init__.py file"""

    # Get current directory parent
    current_dir = Path(__file__).resolve().parents[1]

    # List of folders to zip
    folders_to_zip = [current_dir.joinpath("operations"), current_dir.joinpath("utils")]

    # Output path
    output_zip_path = current_dir.joinpath('modules.zip')

    # Zip the modules Folders
    zipit(folders_to_zip, output_zip_path)

    # add __init__.py
    with zipfile.ZipFile(output_zip_path, 'a') as zipf:
        logger.info("Ziping __init__.py file")
        
        source_path = current_dir.joinpath("__init__.py")
        destination = 'stocks_etl/__init__.py'
        zipf.write(source_path, destination)