#!/usr/bin/env python3
''' Convert a "The Story Graph" export csv to dataframe '''
import argparse
import logging
import pandas as pd


class StoryGraphExportCleaner:
    ''' Convert a "The Story Graph" export CSV to DataFrame '''
    def __init__(self, input_file, output_file, log_level='INFO'):
        logging.basicConfig(
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.input_file = input_file
        self.output_file = output_file or input_file.replace("csv", "json")
        self.df = None

    def split_content_warnings(self, row):
        ''' Convert contentWarnings dict into 3 separate columns '''
        cw_graphic = []
        cw_moderate = []
        cw_minor = []
        self.logger.debug(
            "Processing row for content warnings: %s",
            row['contentWarnings']
        )

        if row['contentWarnings'].get("Graphic"):
            cw_graphic = row['contentWarnings']["Graphic"]
        if row['contentWarnings'].get("Moderate"):
            cw_moderate = row['contentWarnings']["Moderate"]
        if row['contentWarnings'].get("Minor"):
            cw_minor = row['contentWarnings']["Minor"]

        return pd.Series([
            cw_graphic,
            cw_moderate,
            cw_minor],
            index=[
                'contentWarningsGraphic',
                'contentWarningsModerate',
                'contentWarningsMinor'
            ]
        )

    def content_warnings_to_dict(self, test_note):
        '''
        Split contentWarnings into dict with keys:
            Graphic, Moderate, Minor
        '''
        my_dict = {}
        if isinstance(test_note, str):
            for line in test_note.split(";"):
                self.logger.debug('line: %s', line)
                if ':' in line:
                    stuff = line.replace('\n', '').replace('\r', '').split(":")
                    self.logger.debug('stuff: %s', stuff)
                    my_dict[stuff[0].strip()] = [
                        x.strip() for x in stuff[1].split(',')
                    ]
        return my_dict

    def process_file(self):
        ''' Pre-process file for import '''
        self.logger.info("Processing file: %s", self.input_file)
        self.df = pd.read_csv(self.input_file)
        self.df.rename(columns={
            'Character- or Plot-Driven?': 'driver',
            'Strong Character Development?': 'charactersDevelopment',
            'Loveable Characters?': 'charactersLoveable',
            'Diverse Characters?': 'charactersDiverse',
            'Flawed Characters?': 'charactersFlawed',
            'Star Rating': 'rating',
            'Date Added': 'dateAdded',
            'Dates Read': 'dateRead',
            'Read Status': 'readStatus',
            'Read Count': 'readCount',
            'Last Date Read': 'lastDateRead',
            'Content Warnings': 'contentWarnings',
            'Content Warning Description': 'contentWarningsDescription',
            'ISBN/UID': 'ISBN',
        }, inplace=True)

        # Change dtypes and clean up contentWarnings
        self.df['Title'] = self.df['Title'].astype('string')
        self.df['Authors'] = self.df[
            'Authors'].str.split(',').fillna('').apply(
            lambda x: [item.strip() for item in x]
        )
        self.df['Contributors'] = self.df[
            'Contributors'].str.split(',').fillna('').apply(
            lambda x: [item.strip() for item in x]
        )
        self.df['ISBN'] = self.df['ISBN'].astype('string')
        self.df['Format'] = self.df['Format'].astype('string')
        self.df['readStatus'] = self.df['readStatus'].astype('string')
        self.df['readCount'] = self.df['readCount'].astype('Int64')
        self.df['Moods'] = self.df['Moods'].astype('string')
        self.df['Moods'] = self.df['Moods'].str.split(',').fillna('').apply(
            lambda x: [item.strip() for item in x]
        )
        self.df['Pace'] = self.df['Pace'].astype('string')
        self.df['driver'] = self.df['driver'].astype('string')
        self.df['charactersDevelopment'] = self.df[
            'charactersDevelopment'].astype('string')
        self.df['charactersLoveable'] = self.df[
            'charactersLoveable'].astype('string')
        self.df['charactersDiverse'] = self.df[
            'charactersDiverse'].astype('string')
        self.df['charactersFlawed'] = self.df[
            'charactersFlawed'].astype('string')
        self.df['Review'] = self.df['Review'].astype('string')
        self.df['contentWarnings'] = self.df[
            'contentWarnings'].astype('string')
        self.df['contentWarnings'] = self.df['contentWarnings'].apply(
            self.content_warnings_to_dict
        )
        new_columns = [
            'contentWarningsGraphic',
            'contentWarningsModerate',
            'contentWarningsMinor'
        ]
        self.df[new_columns] = self.df.apply(
            self.split_content_warnings, axis=1
        )
        self.df.drop('contentWarnings', axis=1, inplace=True)
        self.df['contentWarningsDescription'] = self.df[
            'contentWarningsDescription'].astype('string')
        self.df['Tags'] = self.df['Tags'].str.split(',').fillna('').apply(
            lambda x: [item.strip() for item in x]
        )
        self.df['Owned?'] = self.df['Owned?'].astype('string')
        self.logger.debug("Processed df.iloc[0]:\n%s", self.df.iloc[0])
        return self.df

    def save_to_json(self):
        ''' Save the DataFrame to a JSON file '''
        self.df.to_json(self.output_file, orient='records', lines=True)
        self.logger.info("DataFrame saved to JSON file: %s", self.output_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Convert The Story Graph export csv to dataframe.' +
        ' This will be used for Obsidian import.'
    )
    parser.add_argument(
        "-f", "--filename", required=True, help="Enter input file"
    )
    parser.add_argument(
        "-o", "--outfile",
        help="The full path name of the output file. Default is to" +
        " change the csv in required filename parameter to json"
    )
    parser.add_argument(
        '--log',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set the logging level (default: INFO)'
    )
    args = parser.parse_args()
    # process directory
    cleaner = StoryGraphExportCleaner(
        args.filename,
        args.outfile,
        args.log.upper()
    )
    cleaner.process_file()
    cleaner.save_to_json()
