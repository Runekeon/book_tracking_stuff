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


class GoodreadsMdProcessor:
    '''
    Get a dataframe from the Goodreads markdown files created with Booksidian
    '''
    def __init__(self, directory, output_file, log_level='INFO'):
        logging.basicConfig(
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.directory = directory
        self.output_file = output_file or date.today().strftime(
            "goodreads_from_md_%Y_%m_%d.json"
        )
        # Create a logger object

        self.yaml_data = {}
        self.df = None

    def get_yaml(self, f):
        ''' Get the YAML data from the file '''
        pointer = f.tell()
        if f.readline() != '---\n':
            f.seek(pointer)
            self.logger.debug("YAML header not found; returning empty data.")
            return ''
        lines = []
        for line in f:
            if line == '---\n':
                break
            lines.append(line)
        return ''.join(lines)

    def filter_data(self, data):
        ''' Remove markdown links from data '''
        self.logger.debug(
            'filter_data data:%s, data type: %s', data, type(data)
        )
        result_data = {}
        if isinstance(data, dict):
            for k, v in data.items():
                self.logger.debug('filter_data k:%s, v type: %s', k, type(v))
                if k is None:
                    continue
                if k == 'author':
                    self.logger.debug(
                        "filter_data author v:%s, v type: %s", v, type(v)
                    )
                    if isinstance(v, list):
                        v = [author.replace('Authors/', '').replace(
                            ']', '').replace('[', '') for author in v]
                    else:
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
        self.logger.info("Reading files from directory: %s", self.directory)
        # Use glob to find all files ending with .yaml or .yml
        files = glob.glob(os.path.join(self.directory, "*.md"))
        for filepath in files:
            self.logger.debug('read_yaml_files filepath:%s', filepath)
            filename = os.path.basename(filepath)
            try:
                with open(filepath, 'r', encoding="utf8") as file:
                    data = yaml.load(self.get_yaml(file), Loader=yaml.Loader)
                    all_data[filename] = self.filter_data(data)
                    self.logger.debug(
                        "Successfully read and filtered data from: %s",
                        filename
                    )
            except yaml.YAMLError as e:
                self.logger.error(
                    "Error reading YAML file %s: %s",
                    filename, e
                )
            except FileNotFoundError:
                self.logger.warning("File not found: %s", filepath)
                continue  # Skip to the next file if one is not found
        self.yaml_data = all_data
        self.logger.info("Total files processed: %d", len(all_data))

    def make_goodreads_df(self):
        ''' Get YAML data and refactor it to create and return a DataFrame '''
        self.read_yaml_files()
        self.logger.debug('make_goodreads_df yaml_data:%s', self.yaml_data)
        final_dict = {}
        for filename, data in self.yaml_data.items():
            self.logger.debug(
                'make_goodreads_df data:%s, data type:%s',
                data, type(data)
            )
            if data is None:
                self.logger.warning('Filename: %s, data is None', filename)
            elif 'id' in data:
                gid = data['id']
                final_dict[gid] = {
                    'filename': filename,
                    **{k: v for k, v in data.items() if k != 'id'}
                }
            else:
                self.logger.warning(
                    'ID missing for filename: %s, data: %s',
                    filename, data
                )
        self.logger.info("make_goodreads_df final_dict len:%s", len(final_dict))
        self.df = pd.DataFrame.from_dict(final_dict, orient='index')
        self.logger.info("make_goodreads_df df.iloc[0] 1:\n%s", self.df.iloc[0])
        self.df.index.name = 'id'
        self.df = self.df.reset_index()
        self.logger.info("make_goodreads_df df.iloc[0] 2:\n%s", self.df.iloc[0])

    def get_goodreads_df(self):
        ''' return DataFrame '''
        if self.df is None:
            self.make_goodreads_df()
        return self.df

    def save_to_json(self):
        ''' Save the DataFrame to a JSON file '''
        self.df.to_json(self.output_file, orient='records', lines=True)
        self.logger.info("DataFrame saved to JSON file: %s", self.output_file)


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
        help="The full path name of the output file. Default is " +
        "goodreads_from_md_Y_m_d.json in current dir"
    )
    parser.add_argument(
        '--log',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set the logging level (default: INFO)'
    )
    args = parser.parse_args()
    # process directory
    processor = GoodreadsMdProcessor(
        args.directory,
        args.file,
        args.log.upper()
    )
    processor.make_goodreads_df()
    processor.save_to_json()
    # directory=args.directory, output_file=args.file, debug=args.debug
