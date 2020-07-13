import os
import time

from datapackage import Package
from etl_server.pipelines.controllers import Controllers as PipelineControllers
from etl_server.users.models import Models as UserModels

etl_pipelines = PipelineControllers({}, os.environ['ETLS_DATABASE_URL'])
etl_users = UserModels(os.environ['ETLS_DATABASE_URL'])

SOURCE='backup/datapackage.json'

p = Package(SOURCE)
it = p.resources[0].iter(keyed=True)

users = etl_users.query()
owner = users['result'][0]['key']
print('OWNER ID:', owner)
pipeline_ids = []
for config in it:
    body = dict(
        name=config['snippets'][0],
        description='\n'.join(config['snippets'][:5]),
        private=False,
        kind='datacity',
        schedule='manual',
        params=dict(
            dgpConfig=config['config'],
        ),
    )
    ret = etl_pipelines.create_or_edit_pipeline(None, body, owner=owner, allowed_all=True)
    pipeline_id = ret['result']['id']
    pipeline_ids.append(pipeline_id)

time.sleep(30)
for pipeline_id in pipeline_ids:
    ret = etl_pipelines.start_pipeline(pipeline_id)
    print(pipeline_id, ret)
    time.sleep(180)
