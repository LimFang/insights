import json
import os
import uuid
import click

import aitw.insights.insights as insights_file
import aitw.scrape.worker as scrape_worker
import aitw.scrape.manager as scrape_manager
import aitw.archive.archive as archive_file
import aitw.scrape.pr_classifier as pr_classifier
import aitw.scrape.export as export_file
import aitw.database.schema as schema_file
import aitw.scrape.pilot as pilot_file

import dotenv
dotenv.load_dotenv(override=True)

@click.group()
def cli():
    pass

@cli.group()
def scrape():
    pass

@scrape.command()
@click.option('--token', default=lambda: os.getenv('GITHUB_TOKEN'), required=True)
@click.option('--id', default=lambda: uuid.uuid4())
@click.option('--group')
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def worker(token, id, group, db):
    scrape_worker.worker(token, id, group, db)
    
@scrape.group()
def manager():
    pass

@manager.command()
@click.argument("group")
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def stats(group, db):
    scrape_manager.stats(group, db)
    
@manager.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def monitor(db):
    scrape_manager.monitor(db)
    
@manager.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def update(db):
    scrape_manager.update(db)
    
@manager.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def backfill(db):
    scrape_manager.backfill(db)
    
@cli.command()
@click.argument('insight', required=True)
@click.option('--db-backend', envvar='POSTGRES_CONNECT_BACKEND', required=True)
@click.option('--db-frontend', envvar='POSTGRES_CONNECT_FRONTEND', required=True)
def insights(insight, db_backend, db_frontend):
    insights_file.insights(insight, db_backend, db_frontend)
    
@cli.group()
def archive():
    pass
    
@archive.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
@click.option('--output', '-o', default='./archive/prs.csv.gz', type=click.Path())
def prs(db, output):
    archive_file.prs(db, output)
    
@archive.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
@click.option('--output', '-o', default='./archive/repos.csv.gz', type=click.Path())
def repos(db, output):
    archive_file.repos(db, output)
    
@archive.command()
@click.option('--output', '-o', default='./archive/website.tar.gz', type=click.Path())
def website(output):
    archive_file.website(output)

@archive.command()
@click.option('--db', envvar='POSTGRES_CONNECT_FRONTEND', required=True)
@click.option('--token', envvar='ZENODO_TOKEN', required=True)
@click.option('--files', multiple=True, 
              default=['./archive/prs.csv.gz', './archive/repos.csv.gz', './archive/website.tar.gz'],
              type=click.Path(exists=True, file_okay=True,))
def upload(db, token, files):
    archive_file.upload(db, token, files)
    
@cli.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def reclassify(db):
    pr_classifier.reclassify(db)

@cli.command('init-db')
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def init_db(db):
    schema_file.init_schema(db)

@cli.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
@click.option('--output', '-o', default='./export/filtered_prs.csv', type=click.Path())
@click.option('--months', default=9, show_default=True)
@click.option('--min-stars', default=500, show_default=True)
def export(db, output, months, min_stars):
    export_file.export_filtered(db, output, months, min_stars)

@cli.group()
def pilot():
    pass

@pilot.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
@click.option('--name', default=pilot_file.RUN_NAME, show_default=True)
def prepare(db, name):
    run_id = pilot_file.ensure_run(db, name)
    click.echo(f'✅ Pilot run {name} ready (id={run_id})')

@pilot.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
@click.option('--name', default=pilot_file.RUN_NAME, show_default=True)
@click.option('--batch', default=10, show_default=True)
@click.option('--poll', default=5, show_default=True)
@click.option('--max-queue', default=15, show_default=True)
def run(db, name, batch, poll, max_queue):
    achieved = pilot_file.run(db, name, batch=batch, poll=poll, max_queue=max_queue)
    click.echo(f'🏁 Pilot finished: {achieved}')

@pilot.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
@click.option('--name', default=pilot_file.RUN_NAME, show_default=True)
def status(db, name):
    pilot_file.status(db, name)

@pilot.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
@click.option('--name', default=pilot_file.RUN_NAME, show_default=True)
def finalize(db, name):
    achieved = pilot_file.finalize(db, name)
    click.echo(f'🏁 Finalized: {achieved}')

@pilot.command('export')
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
@click.option('--name', default=pilot_file.RUN_NAME, show_default=True)
@click.option('--output', '-o', default='data/export/pilot', type=click.Path())
def export_cmd(db, name, output):
    summary = pilot_file.export(db, name, output)
    click.echo(json.dumps(summary['agents'], indent=2, ensure_ascii=False))

if __name__ == '__main__':
    cli()