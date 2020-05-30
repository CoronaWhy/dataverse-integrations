from config import DV_ALIAS, BASE_URL, API_TOKEN, REPO, GITHUB_TOKEN, PARSABLE_EXTENSIONS, gitroot, gituserroot, gitblob, gitmaster
from syncdataverse import DataverseData
from github import Github
from pyDataverse.api import Api,NativeApi
from pyDataverse.models import Datafile, Dataset

repos = []
REPO='Mike-Honey/covid-19-vic-au'
repos.append(REPO)
REPO='daenuprobst/covid19-cases-switzerland'
repos.append(REPO)

for thisrepo in repos:
    print(thisrepo)
    dvn = DataverseData(thisrepo, True)
    print(dvn.collect_urls())
    dvn.datasync()

