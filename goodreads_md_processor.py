#!/usr/bin/env python3
'''
    Booksidian is the best way to get GoodReads data
    I want to link it to The Story Graph data so need to get
    a dataframe from the Goodreads markdown files
'''

import argparse
from datetime import date
import glob
import logging
import os

import pandas as pd
import yaml


# Configure logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')


class GoodreadsMdProcessor:
    '''
    Get a dataframe from the Goodreads markdown files created with Booksidian
    '''
    def __init__(self, directory, output_file):
        self.directory = directory
        self.output_file = output_file
        self.yaml_data = {}

    def get_yaml(self, f):
        ''' Get the YAML data from the file '''
        pointer = f.tell()
        if f.readline() != '---\n':
            f.seek(pointer)
            logger.debug("YAML header not found; returning empty data.")
            return ''
        lines = []
        for line in f:
            if line == '---\n':
                break
            lines.append(line)
        return ''.join(lines)

    def filter_data(self, data):
        ''' Remove markdown links from data '''
        logger.debug('filter_data data:%s, data type: %s', data, type(data))
        result_data = {}
        if isinstance(data, dict):
            for k, v in data.items():
                logger.debug('filter_data k:%s, v type: %s', k, type(v))
                if k is None:
                    continue
                if k == 'author':
                    logger.debug(
                        "filter_data author v:%s, v type: %s", v, type(v)
                    )
                    v = v.replace('Authors/', '').replace(
                        ']', '').replace('[', '')
                if k == 'shelves':
                    shelves = [
                        shelf.replace('Shelves/', '').replace(']', '')
                        .replace('[', '') for shelf in v
                    ]
                    v = shelves
                result_data[k] = v
        return result_data

    def read_yaml_files(self):
        """
        Reads all YAML files in a directory and returns a dictionary
        where keys are filenames and values are the loaded YAML data.
        """
        all_data = {}
        logger.info("Reading files from directory: %s", self.directory)
        # Use glob to find all files ending with .yaml or .yml
        files = glob.glob(os.path.join(self.directory, "*.md"))
        for filepath in files:
            logger.debug('read_yaml_files filepath:%s', filepath)
            filename = os.path.basename(filepath)
            try:
                with open(filepath, 'r', encoding="utf8") as file:
                    data = yaml.load(self.get_yaml(file), Loader=yaml.Loader)
                    all_data[filename] = self.filter_data(data)
                    logger.info(
                        "Successfully read and filtered data from: %s",
                        filename
                    )
            except yaml.YAMLError as e:
                logger.error(
                    "Error reading YAML file %s: %s",
                    filename, e
                )
            except FileNotFoundError:
                logger.warning("File not found: %s", filepath)
                continue  # Skip to the next file if one is not found
        self.yaml_data = all_data
        logger.info("Total files processed: %d", len(all_data))

    def get_goodreads_df(self):
        ''' Get YAML data and refactor it to create and return a DataFrame '''
        self.read_yaml_files()
        logger.debug('get_goodreads_df yaml_data:%s', self.yaml_data)
        final_dict = {}
        for filename, data in self.yaml_data.items():
            logger.debug(
                'get_goodreads_df data:%s, data type:%s',
                data, type(data)
            )
            if data is None:
                logger.warning('Filename: %s, data is None', filename)
            elif 'id' in data:
                gid = data['id']
                final_dict[gid] = {
                    'filename': filename,
                    **{k: v for k, v in data.items() if k != 'id'}
                }
            else:
                logger.warning(
                    'ID missing for filename: %s, data: %s',
                    filename, data
                )

        df = pd.DataFrame.from_dict(final_dict, orient='index')
        df.index.name = 'id'
        df = df.reset_index()
        logger.info("DataFrame created successfully.")
        logger.debug("DataFrame info:\n%s", df.info())
        return df

    def save_to_json(self, df):
        ''' Save the DataFrame to a JSON file '''
        df.to_json(self.output_file, orient='records', lines=True)
        logger.info("DataFrame saved to JSON file: %s", self.output_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Pre The Story Graph export csv for obsidian import'
    )
    parser.add_argument(
        "-d", "--directory", required=True,
        help="The directory where Goodreads markdown files are stored"
    )
    parser.add_argument(
        "-f", "--file",
        default=date.today().strftime("goodreads_from_md_%Y_%m_%d.json"),
        help="The full path name of the output file. Default is " +
        "goodreads_from_md_Y_m_d.json in current dir"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    # Configure logging based on the debug flag
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    # Create a logger object
    logger = logging.getLogger(__name__)

    # process directory
    processor = GoodreadsMdProcessor(args.directory, args.file)
    gr_df = processor.get_goodreads_df()
    processor.save_to_json(gr_df)
