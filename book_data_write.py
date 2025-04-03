#!/usr/bin/env python3
''''
    combine data from Goodreads and The Story Graph
'''
import argparse
import logging
import os
import pathlib
import string
import yaml
from book_data_integrator import BookDataIntegrator


AUTHOR_TEMPLATE = string.Template("""---
tsg_last_checked:
tsg_count:
gr_last_checked:
gr_count:
---

````dataviewjs
let bookCount = dv.pages('"Combined"').where((p) => dv.func.contains(String(p.shelves), "$author")).length
let readCount = dv.pages('"Combined"').where((p) => dv.func.contains(String(p.shelves), "$author") && dv.func.contains(String(p.shelves), 'Shelves/read')).length
const percentRead = dv.func.round((readCount / bookCount) * 100, 2)

dv.header(2, `Books: ${bookCount}, Read: ${readCount}, Percent Read: ${percentRead}%`)
````

```dataview
TABLE isbn, datePublished, dateAdded, dateRead, avgRating, rating, pages, shelves, series
FROM "Combined"
WHERE contains(string(shelves), "$author")=true
SORT series ASC
```
""")

SERIES_TEMPLATE = string.Template("""---
tsg_last_checked:
tsg_count:
gr_last_checked:
gr_count:
---

````dataviewjs
let bookCount = dv.pages('"Combined"').where((p) => dv.func.contains(String(p.shelves), "$ser")).length
let readCount = dv.pages('"Combined"').where((p) => dv.func.contains(String(p.shelves), "$ser") && dv.func.contains(String(p.shelves), 'Shelves/read')).length
const percentRead = dv.func.round((readCount / bookCount) * 100, 2)

dv.header(2, `Books: ${bookCount}, Read: ${readCount}, Percent Read: ${percentRead}%`)
````

```dataview
TABLE isbn, datePublished, dateAdded, dateRead, avgRating, rating, pages, shelves, series
FROM "Combined"
WHERE contains(string(shelves), "$ser")=true
SORT series ASC
```
""")

SHELVES_TEMPLATE = string.Template("""---
tsg_last_checked:
tsg_count:
gr_last_checked:
gr_count:
---

````dataviewjs
let bookCount = dv.pages('"Combined"').where((p) => dv.func.contains(String(p.shelves), "$tag")).length
let readCount = dv.pages('"Combined"').where((p) => dv.func.contains(String(p.shelves), "$tag") && dv.func.contains(String(p.shelves), 'Shelves/read')).length
const percentRead = dv.func.round((readCount / bookCount) * 100, 2)

dv.header(2, `Books: ${bookCount}, Read: ${readCount}, Percent Read: ${percentRead}%`)
````

```dataview
TABLE isbn, datePublished, dateAdded, dateRead, avgRating, rating, pages, shelves, series
FROM "Combined"
WHERE contains(string(shelves), "$tag")=true
SORT series ASC
```
""")


class BookDataWriter:
    ''''
    use BookDataIntegrator dataframes:
    1) create new files for obsidian in "{vault_directory}/Combined"
    2) Delete overlapping files in "{vault_directory}/Goodreads"
    3) Delete overlapping files in "{vault_directory}/StoryGraph"
    '''
    def __init__(
            self,
            vault_directory,
            story_graph_file,
            log_level='INFO'
    ):
        logging.basicConfig(
            format='%(asctime)s,%(msecs)03d %(levelname)-8s '
            '[%(filename)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d:%H:%M:%S',
        )
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.logger.info(
            "vault_directory: %s, story_graph_file: %s, log_level: %s",
            vault_directory,
            story_graph_file,
            'INFO'
        )
        self.merged_df = None
        self.vault_directory = vault_directory
        self.integrator = BookDataIntegrator(
            f'{vault_directory}/Goodreads/',
            story_graph_file,
            'should_not_print.json',
            'INFO'
        )

    def get_integrator_data(self):
        """Fetch the Integrator data as a DataFrame."""
        self.integrator.process()
        self.merged_df = self.integrator.get_merged_df()
        self.logger.info(
            "merged_df.iloc[0] 1:\n%s", self.merged_df.iloc[0]
        )

    def delete_old_md(self, filename):
        """ delete old markdown files in the vault directory """
        pathlib.Path(filename).unlink(missing_ok=True)

    def remove_non_printable(self, text):
        """Removes non-printable characters from a string.

        Args:
            text: The input string.

        Returns:
            The string with non-printable characters removed.
        """
        return ''.join(char for char in text if char in string.printable)

    def get_unique_values(self, *lists):
        """ get unique values from a list of lists and strings """
        combined_list = []
        for lst in lists:
            if isinstance(lst, list):
                combined_list.extend(lst)
            else:
                combined_list.append(lst)
        unique_values = list(set(combined_list))
        return unique_values

    def clean_title(self, title1, title2):
        """ remove characters from title that are not in both """
        title1 = self.remove_non_printable(title1)
        title2 = self.remove_non_printable(title2)
        if ':' in title1:
            title1 = title1.split(':', maxsplit=1)
        if ':' in title2:
            title2 = title2.split(':', maxsplit=1)
        for char in '[](){}_':
            title1 = title1.replace(char, '')
            title2 = title2.replace(char, '')
        combined_string = title1 + title1
        unique_chars = set(combined_string)
        self.logger.info(
            "unique_chars: %s unique_chars length: %s",
            unique_chars, len(unique_chars),
        )

        for char in unique_chars:
            if char in title1 and char in title2:
                continue
            title1 = title1.replace(char, '')
            title2 = title2.replace(char, '')
        self.logger.info(
            "title1: %s title1 length: %s, title2: %s title2 length: %s",
            title1, len(title1),
            title2, len(title2),
        )
        if title1 == title2:
            return title1
        return f'*{title1} + {title2}*'

    def write_author_files(self, authors):
        ''' write shelf files '''
        for author in authors:
            shelf_file = os.path.join(
                self.vault_directory,
                '/Authors/',
                f'{author}.md')
            with open(shelf_file, "w", encoding="utf-8") as file:
                file.write(AUTHOR_TEMPLATE.substitute({"author": author}))

    def write_series_files(self, series):
        ''' write shelf files '''
        for ser in series:
            series_file = os.path.join(
                self.vault_directory,
                '/Series/',
                f'{ser}.md')
            with open(series_file, "w", encoding="utf-8") as file:
                file.write(SERIES_TEMPLATE.substitute({"ser": ser}))

    def write_tag_files(self, tags):
        ''' write shelf files '''
        for tag in tags:
            tag_file = os.path.join(
                self.vault_directory,
                '/Shelves/',
                f'{tag}.md')
            with open(tag_file, "w", encoding="utf-8") as file:
                file.write(SHELVES_TEMPLATE.substitute({"tag": tag}))

    def process_integrator_data(self):
        """
            loop over Integrator data and print row
            print out to file in Obsidian format
        """
        unique_authors = set()
        unique_series = set()
        unique_tags = set()
        for _, row in self.merged_df.iterrows():
            if row['story_graph_Title'] == row['goodreads_title']:
                title = row['story_graph_Title']
            else:
                title = self.clean_title(
                    row['goodreads_title'], row['story_graph_Title']
                )
            outfile = os.path.join(
                self.vault_directory,
                '/Combined/',
                f'{title}.md')
            self.logger.info(
                "outfile: %s", outfile
            )
            with open(outfile, "w", encoding="utf-8") as file:
                file.write(yaml.dump(row.to_dict(), sort_keys=False))
            old_gr_file = os.path.join(
                self.vault_directory,
                '/Goodreads/',
                f'{row['goodreads_title']} - {row['id']}.md')
            self.delete_old_md(old_gr_file)
            old_gr_file = os.path.join(
                self.vault_directory,
                '/StoryGraph/',
                f'{row['story_graph_Title']}.md')
            self.delete_old_md(old_gr_file)
            for item in self.get_unique_values(
                row['Authors'], row['author']
            ):
                unique_authors.add(item)
            for item in self.get_unique_values(
                row['seriesName'], row['series']
            ):
                unique_series.add(item)
            for item in self.get_unique_values(
                row['readStatus'], row['shelves'], row['Tags']
            ):
                unique_tags.add(item)

        self.write_author_files(unique_authors)
        self.write_series_files(unique_series)
        self.write_tag_files(unique_tags)

    def process(self):
        """
        Fetch data and process, then output or return the merged DataFrame.
        """
        self.get_integrator_data()
        self.process_integrator_data()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Integrate Goodreads and Story Graph data.'
    )
    parser.add_argument(
        '-d', '--vault_directory', required=True,
        default='/d/Books_Vault/',
        help='The directory containing obsidian vault.'
        ' It should have directories for Combined, Goodreads and StoryGraph'
        ' markdown files.'
    )
    parser.add_argument(
        '-f', '--storygraph_file', required=True,
        help='The CSV file for Story Graph data.'
    )
    parser.add_argument(
        '--log',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set the logging level (default: INFO)'
    )
    args = parser.parse_args()
    # process directory
    BookWriter = BookDataWriter(
        args.vault_directory,
        args.storygraph_file,
        args.log.upper()
    )
    BookWriter.process()
