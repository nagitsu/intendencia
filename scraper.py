import click
import functools
import json
import os.path
import random
import requests
import time

from click import echo, progressbar
from lxml import html, etree
from multiprocessing.dummy import Pool
from urllib.parse import urljoin


BASE_URL = 'http://www.montevideo.gub.uy/asl/sistemas/Gestar/resoluci.nsf'


def fetch_dates():
    url = f'{BASE_URL}/BetaWebFechaApAsc?OpenView&Start=1&Count=30000'

    response = get(url)
    root = html.fromstring(response.content)

    dates = root.xpath("//font[@size='2' and @face='Arial']/text()")

    return dates


def get(url, retries=5):
    tries = 0
    while tries < retries:
        try:
            return requests.get(url)
        except Exception as e:
            tries += 1
            time.sleep(1)


def fetch_resolution_urls_for_day(date):
    url = (
        f'{BASE_URL}/BetaWebFechaAp?OpenView&RestrictToCategory={date}'
        '&ExpandView&Count=30000'
    )

    response = get(url)
    root = html.fromstring(response.content)

    relative_urls = root.xpath("//font[@size='2' and @face='Arial']/a/@href")
    urls = [urljoin(BASE_URL, url) for url in relative_urls]

    return urls

def clean(text):
    return " ".join(text.split()).strip()


def fetch_resolution(url, date, save_html=True):
    response = get(url)
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

        'category': clean("".join(root.xpath(selectors['category']))),
        'subcategory': clean("".join(root.xpath(selectors['subcategory']))),
        'summary': clean("".join(root.xpath(selectors['summary']))),

        'content': clean("".join(root.xpath(selectors['content']))),
        'url': url,
        'date': date,
    }

    if save_html:
        resolution['html'] = etree.tostring(root, encoding='unicode'),

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


def fetch_resolutions_for_day(date, save_html=False):
    urls = fetch_resolution_urls_for_day(date)
    return [fetch_resolution(url, date, save_html=save_html) for url in urls]


def write_to_file(resolutions, folder):
    date = resolutions[0]['date']
    filename = os.path.join(folder, date + '.jsonl')
    dirname = os.path.dirname(filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    if not os.path.exists(filename):
        with open(filename, 'w'):
            pass

    resolutions.sort(key=lambda x: x['resolution_number'])

    with open(filename, 'a') as f:
        for resolution in resolutions:
            f.write(json.dumps(resolution, ensure_ascii=False, sort_keys=True) + '\n')


@click.command()
@click.argument('folder', default='resolutions/', type=str)
@click.option('--concurrency', default=1, type=int)
@click.option('--limit', default=None, type=int)
@click.option('--no-html', is_flag=True)
def cli(folder, concurrency, limit, no_html):
    dates = fetch_dates()
    echo(f'Found {len(dates)} dates to parse')

    if limit:
        dates = random.sample(dates, limit)

    fetch_resolutions_for_day_fn = functools.partial(fetch_resolutions_for_day, save_html=not no_html)

    count = 0
    with Pool(processes=concurrency) as pool:
        with progressbar(dates, label='Fetching dates') as bar:
            fetcher = pool.imap_unordered(fetch_resolutions_for_day_fn, dates)
            for resolutions in fetcher:
                write_to_file(resolutions, folder)
                count += len(resolutions)

                # `imap_unordered` consumes the bar before actually being
                # ready, so we update manually.
                bar.update(1)

    echo(f'{count} resolutions parsed')


if __name__ == '__main__':
    cli()
