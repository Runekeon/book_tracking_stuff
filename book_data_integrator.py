#!/usr/bin/env python3
''''
    combine data from Goodreads and The Story Graph
'''
import argparse
import logging
import pandas as pd
from fuzzywuzzy import fuzz
from goodreads_md_processor import GoodreadsMdProcessor
from story_graph_export_cleaner import StoryGraphExportCleaner


class BookDataIntegrator:
    ''''
    Combine dataframes from
        goodreads_md_processor and story_graph_export_cleaner
    '''
    def __init__(
            self, goodreads_directory, story_graph_file,
            output_file, log_level='INFO'):
        logging.basicConfig(
            format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s', datefmt='%Y-%m-%d:%H:%M:%S',
        )
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.logger.info(
            "goodreads_directory: %s, story_graph_file: %s, output_file: %s, log_level: %s",
            goodreads_directory,
            story_graph_file,
            output_file, log_level
        )
        self.goodreads_df = None
        self.story_graph_df = None
        self.merged_df = None
        self.goodreads_processor = GoodreadsMdProcessor(
            goodreads_directory, 'gr_should_not_print.json', log_level
        )
        self.story_graph_cleaner = StoryGraphExportCleaner(
            story_graph_file, 'tsg_should_not_print.json', log_level
        )
        self.output_file = output_file or story_graph_file.replace(
            "csv", "json"
        )

    def get_goodreads_data(self):
        """Fetch the Goodreads data as a DataFrame."""
        self.goodreads_df = self.goodreads_processor.get_goodreads_df()
        self.logger.debug(
            "goodreads_df.iloc[0] 1:\n%s", self.goodreads_df.iloc[0]
        )
        self.goodreads_df.rename(columns={
            'title': 'goodreads_title',
            'isbn': 'goodreads_isbn',
            'review': 'goodreads_review',
            'rating': 'goodreads_rating',
            'dateAdded': 'goodreads_dateAdded',
            'dateRead': 'goodreads_dateRead',
        }, inplace=True)
        self.logger.debug(
            "goodreads_df.iloc[0] 2:\n%s", self.goodreads_df.iloc[0]
        )

    def get_story_graph_data(self):
        """Fetch the StoryGraph data as a DataFrame."""
        self.story_graph_df = self.story_graph_cleaner.process_file()
        self.logger.debug(
            "story_graph_df.iloc[0] 1:\n%s", self.story_graph_df.iloc[0]
        )
        self.story_graph_df.rename(columns={
            'Title': 'story_graph_Title',
            'ISBN': 'story_graph_isbn',
            'dateAdded': 'story_graph_dateAdded',
            'dateRead': 'story_graph_dateRead',
            'rating': 'story_graph_rating',
            'Review': 'story_graph_Review',
        }, inplace=True)
        self.logger.debug(
            "story_graph_df.iloc[0] 2:\n%s", self.story_graph_df.iloc[0]
        )

    def join_dataframes(self):
        """
        Join the dataframes based on ISBN or fuzzy match on Title and Authors.
        """
        # filter ISBN remove na and empty string
        goodreads_isbn = self.goodreads_df[
            (self.goodreads_df['goodreads_isbn'].notna()) & \
            (self.goodreads_df['goodreads_isbn'] != '') & \
            (self.goodreads_df['goodreads_isbn'].notnull())
        ]
        self.logger.info(
            "goodreads_isbn length: %s, iloc[0]:\n%s, ",
                len(goodreads_isbn),
                goodreads_isbn.iloc[0]
        )
        storygraph_isbn = self.story_graph_df[
            (self.story_graph_df['story_graph_isbn'].notna()) & \
            (self.story_graph_df['story_graph_isbn'] != '') & \
            (self.story_graph_df['story_graph_isbn'].notnull())
        ]
        self.logger.info(
            "storygraph_isbn length: %s, iloc[0]:\n%s, ",
                len(storygraph_isbn),
                storygraph_isbn.iloc[0]
        )

        # Attempt to merge on ISBN first
        self.merged_df = pd.merge(
            goodreads_isbn, storygraph_isbn,
            left_on='goodreads_isbn', right_on='story_graph_isbn',
            how='inner',
            indicator=True
        )
        self.logger.info(
            "after ISBN merged_df length: %s, sg length: %s, iloc[0]:\n%s, ",
                len(self.merged_df),
                len(self.merged_df[
                    self.merged_df['story_graph_isbn'].notnull().notna()
                ]),
                self.merged_df.iloc[0]
        )

        # Find rows from both DataFrames that didn't match on ISBN
        goodreads_no_isbn = self.goodreads_df[
            ~self.goodreads_df['goodreads_isbn'].isin(self.merged_df['goodreads_isbn'])
        ]
        self.logger.info(
            "goodreads_no_isbn length: %s, iloc[0]:\n%s, ",
                len(goodreads_no_isbn),
                goodreads_no_isbn.iloc[0]
        )

        storygraph_no_isbn = self.story_graph_df[
            ~self.story_graph_df['story_graph_isbn'].isin(self.merged_df['story_graph_isbn'])
        ]
        self.logger.info(
            "storygraph_no_isbn length: %s, iloc[0]:\n%s, ",
                len(storygraph_no_isbn),
                storygraph_no_isbn.iloc[0]
        )

        fuzzy_matches = []

        # Fuzzy matching for Title and Authors
        for _, story_row in storygraph_no_isbn.iterrows():
            self.logger.debug("tsg loop top: %s", story_row)
            title = story_row['story_graph_Title']
            authors = story_row['Authors'] if isinstance(
                story_row['Authors'], list) else [story_row['Authors']]
            review = story_row['story_graph_Review']

            for _, goodreads_row in goodreads_no_isbn.iterrows():
                self.logger.debug("gr loop top: %s", goodreads_row)
                goodreads_title = goodreads_row['goodreads_title']
                goodreads_authors = goodreads_row['author'] if isinstance(
                    goodreads_row['author'], list
                ) else [goodreads_row['author']]
                goodreads_review = goodreads_row['goodreads_review']

                self.logger.debug(
                    'title type: %s, goodreads_title type: %s',
                    type(title),
                    type(goodreads_title)
                )
                self.logger.debug(
                    'authors type: %s, goodreads_authors type: %s',
                    type(authors),
                    type(goodreads_authors)
                )
                self.logger.debug(
                    'review type: %s, goodreads_review type: %s',
                    type(review),
                    type(goodreads_review)
                )
                title_match = False
                review_match = False
                author_match = False
                if isinstance(title, str) and isinstance(goodreads_title, str):
                    title_match = fuzz.partial_ratio(
                        title.lower(), goodreads_title.lower()) >= 80
                    self.logger.debug("title_match type: %s", type(title_match))
                if isinstance(review, str) and isinstance(goodreads_review, str):
                    review_match = fuzz.partial_ratio(
                        review.lower(), goodreads_review.lower()) >= 80
                    self.logger.debug("review_match type: %s", type(review_match))
                if isinstance(authors, list) and len(authors) != 0 and \
                    isinstance(goodreads_authors, list) and \
                    len(goodreads_authors) != 0:

                    author_match = any(
                        fuzz.partial_ratio(
                            author.lower(), ga.lower()
                        ) >= 80 for ga in goodreads_authors for author in authors
                    )
                    self.logger.debug("author_match type: %s", type(author_match))

                if title_match and author_match:
                    fuzzy_matches.append(
                        pd.concat([goodreads_row, story_row]).to_frame().T
                    )
                elif review_match:
                    fuzzy_matches.append(
                        pd.concat([goodreads_row, story_row]).to_frame().T
                    )

        if fuzzy_matches:
            self.logger.debug(
                "fuzzy_matches length: %s, iloc[0]:\n%s, ",
                    len(fuzzy_matches),
                    fuzzy_matches[0]
            )

            fuzzy_df = pd.concat(fuzzy_matches, ignore_index=True)
            self.logger.debug(
                "fuzzy_df length: %s, iloc[0]:\n%s, ",
                    len(fuzzy_df),
                    fuzzy_df.iloc[0]
            )
            self.logger.debug(
                "end merged_df length: %s, iloc[0]:\n%s, ",
                    len(self.merged_df),
                    self.merged_df.iloc[0]
            )

            self.merged_df = pd.concat(
                [self.merged_df, fuzzy_df],
                ignore_index=True
            )
            self.logger.debug(
                "end merged_df length: %s, iloc[0]:\n%s, ",
                    len(self.merged_df),
                    self.merged_df.iloc[0]
            )

    def save_to_json(self):
        """Save merged DataFrame to JSON file."""
        self.merged_df.to_json(self.output_file, orient='records', lines=True)
        self.logger.info("DataFrame saved to JSON file: %s", self.output_file)

    def process(self):
        """
        Fetch data and process, then output or return the merged DataFrame.
        """
        self.get_goodreads_data()
        self.get_story_graph_data()
        self.join_dataframes()

    def get_merged_df(self):
        ''' return DataFrame '''
        if self.merged_df is None:
            self.process()
        return self.merged_df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Integrate Goodreads and Story Graph data.'
    )
    parser.add_argument(
        '-d', '--goodreads_directory', required=True,
        help='The directory containing Goodreads markdown files.'
    )
    parser.add_argument(
        '-f', '--storygraph_file', required=True,
        help='The CSV file for Story Graph data.'
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
    integrator = BookDataIntegrator(
        args.goodreads_directory,
        args.storygraph_file,
        args.outfile,
        args.log.upper()
    )
    integrator.process()
    integrator.save_to_json()
