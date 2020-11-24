from config import DV_ALIAS, BASE_URL, API_TOKEN, REPO, GITHUB_TOKEN, PARSABLE_EXTENSIONS, gitroot, gituserroot, gitblob, gitmaster
from syncdataverse import DataverseData
from github import Github
from pyDataverse.api import Api,NativeApi
from pyDataverse.models import Datafile, Dataset

repos = []
q = 'covid kg'
q = 'covid'
dvn = DataverseData(REPO)

for repo in dvn.githubsearch(q)[0:10]:
    try:
        print(repo.full_name)
        if not dvn.if_exist(repo.full_name):
            repos.append(repo.full_name)
    except:
        print("something wrong with %s" % repo.full_name)

for thisrepo in repos:
    print(thisrepo)
    dvn = DataverseData(thisrepo, True)
    print(dvn.collect_urls())
    if dvn.urls_found:
        dvn.datasync()

