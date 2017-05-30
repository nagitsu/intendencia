import click
import json
import random
import requests

from click import echo, progressbar
from lxml import html, etree
from multiprocessing.dummy import Pool
from urllib.parse import urljoin


BASE_URL = 'http://www.montevideo.gub.uy/asl/sistemas/Gestar/resoluci.nsf'


def fetch_dates():
    url = f'{BASE_URL}/BetaWebFechaApAsc?OpenView&Start=1&Count=30000'

    response = requests.get(url)
    root = html.fromstring(response.content)

    dates = root.xpath("//font[@size='2' and @face='Arial']/text()")

    return dates


def fetch_resolution_urls_for_day(date):
    url = (
        f'{BASE_URL}/BetaWebFechaAp?OpenView&RestrictToCategory={date}'
        '&ExpandView&Count=30000'
    )

    response = requests.get(url)
    root = html.fromstring(response.content)

    relative_urls = root.xpath("//font[@size='2' and @face='Arial']/a/@href")
    urls = [urljoin(BASE_URL, url) for url in relative_urls]

    return urls


def fetch_resolution(url):
    response = requests.get(url)
    root = html.fromstring(response.content)

    selectors = {
        'resolution_number': "//td[@class='CuerpoResol']/table[1]/tr[1]/td[1]/b[2]/font/text()",  # noqa
        'file_id': "//td[@class='CuerpoResol']/table[1]/tr[1]/td[2]/div/b//text()",  # noqa
        'approval_date': "//td[@class='CuerpoResol']/table[1]/tr[2]/td[2]/div/b//text()",  # noqa

        'category': "//td[@class='CuerpoResol']/table[1]/tr[2]/td[1]/b[1]/font/text()",  # noqa
        'subcategory': "//td[@class='CuerpoResol']/b[1]//text()",  # noqa
        'summary': "//td[@class='CuerpoResol']/b[2]//text()",  # noqa

        'content': "//td[@class='CuerpoResol']/table[3]/tr[1]//text()",  # noqa
        'authors': "//td[@class='CuerpoResol']/table[3]/tr[position() > 1]",  # noqa
    }

    resolution = {
        'resolution_number': "".join(root.xpath(selectors['resolution_number'])),  # noqa
        'file_id': "".join(root.xpath(selectors['file_id'])),
        'approval_date': "".join(root.xpath(selectors['approval_date'])),

        'category': "".join(root.xpath(selectors['category'])),
        'subcategory': "".join(root.xpath(selectors['subcategory'])),
        'summary': "".join(root.xpath(selectors['summary'])),

        'content': "".join(root.xpath(selectors['content'])),
        'html': etree.tostring(root, encoding='unicode'),
        'url': url,
    }

    authors = []
    author_nodes = root.xpath(selectors['authors'])
    for node in author_nodes:
        if not node.text_content().strip():
            continue

        # Remove trailing `.-` for job titles, `,` for names.
        authors.append({
            'name': "".join(node.xpath(".//font[1]/text()")).strip()[:-1],  # noqa
            'job_title': "".join(node.xpath(".//font[position() > 1]/text()")).strip()[:-2],  # noqa
        })

    resolution['authors'] = authors

    return resolution


def fetch_resolutions_for_day(date):
    urls = fetch_resolution_urls_for_day(date)
    return [fetch_resolution(url) for url in urls]


@click.command()
@click.argument('output')
@click.option('--concurrency', default=1, type=int)
@click.option('--limit', default=None, type=int)
def cli(output, concurrency, limit):
    dates = fetch_dates()
    echo(f'Found {len(dates)} dates to parse')

    if limit:
        dates = random.sample(dates, limit)

    # Clear the output file.
    with open(output, 'w') as f:
        pass

    count = 0
    with Pool(processes=concurrency) as pool:
        with progressbar(dates, label='Fetching dates') as bar:
            fetcher = pool.imap_unordered(fetch_resolutions_for_day, dates)
            for resolutions in fetcher:
                with open(output, 'a') as f:
                    for resolution in resolutions:
                        count += 1
                        f.write(json.dumps(resolution) + '\n')

                # `imap_unordered` consumes the bar before actually being
                # ready, so we update manually.
                bar.update(1)

    echo(f'{count} resolutions parsed')


if __name__ == '__main__':
    cli()
